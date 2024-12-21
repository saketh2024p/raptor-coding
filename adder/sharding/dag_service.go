// Package sharding implements a sharding ClusterDAGService places
// content in different shards while it's being added, creating
// a final Cluster DAG and pinning it.
package sharding

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/adder"
	ec "github.com/ipfs-cluster/ipfs-cluster/adder/erasure"
	"github.com/ipfs-cluster/ipfs-cluster/api"

	humanize "github.com/dustin/go-humanize"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

var logger = logging.Logger("shardingdags")

// DAGService is an implementation of a ClusterDAGService which
// shards content while adding among several IPFS Cluster peers,
// creating a Cluster DAG to track and pin that content selectively
// in the IPFS daemons allocated to it.
type DAGService struct {
	adder.BaseDAGService

	ctx       context.Context
	rpcClient *rpc.Client

	addParams api.AddParams
	output    chan<- api.AddedOutput

	addedSet *cid.Set

	// Current shard being built
	currentShard *shard
	// Last flushed shard CID
	previousShard cid.Cid

	// shard tracking
	shards map[string]cid.Cid

	startTime time.Time
	totalSize uint64
	// erasure coding
	rs *ec.Raptor
	// record blocksize to metadata, enable erasure find each block
	blockMeta []ECBlockMeta
}

// New returns a new ClusterDAGService, which uses the given rpc client to perform
// Allocate, IPFSStream and Pin requests to other cluster components.
func New(ctx context.Context, rpc *rpc.Client, opts api.AddParams, out chan<- api.AddedOutput) *DAGService {
	// use a default value for this regardless of what is provided.
	opts.Mode = api.PinModeRecursive
	return &DAGService{
		ctx:       ctx,
		rpcClient: rpc,
		addParams: opts,
		output:    out,
		addedSet:  cid.NewSet(),
		shards:    make(map[string]cid.Cid),
		startTime: time.Now(),
		rs:        ec.NewRaptor(ctx, opts.DataShards, opts.ParityShards, int(opts.ShardSize)),
		blockMeta: make([]ECBlockMeta, 0, 1024),
	}
}

// Add puts the given node in its corresponding shard and sends it to the
// destination peers.
func (dgs *DAGService) Add(ctx context.Context, node ipld.Node) error {
	if !dgs.addedSet.Visit(node.Cid()) {
		return nil
	}

	return dgs.ingestBlock(ctx, node)
}

// Close performs cleanup and should be called when the DAGService is not
// going to be used anymore.
func (dgs *DAGService) Close() error {
	if dgs.currentShard != nil {
		dgs.currentShard.Close()
	}
	return nil
}

// Finalize finishes sharding, creates the cluster DAG and pins it along
// with the meta pin for the root node of the content.
func (dgs *DAGService) Finalize(ctx context.Context, dataRoot api.Cid) (api.Cid, error) {
	shardMeta := make(map[string]cid.Cid, len(dgs.shards))
	for i, ci := range dgs.shards {
		shardMeta[i] = ci
	}
	if dgs.addParams.Erasure {
		parityCids := dgs.rs.GetParityShards()
		for id, ci := range parityCids {
			i, _ := strconv.Atoi(id)
			shardMeta[fmt.Sprintf("%d", i+len(dgs.shards))] = ci
		}
	}
	// clusterDAG
	clusterDAGNodes, err := MakeDAG(ctx, shardMeta)
	if err != nil {
		return dataRoot, err
	}
	blocks := make(chan api.NodeWithMeta, 256)
	go func() {
		defer close(blocks)
		for _, n := range clusterDAGNodes {
			select {
			case <-ctx.Done():
				logger.Error(ctx.Err())
				return //abort
			case blocks <- adder.IpldNodeToNodeWithMeta(n):
			}
		}
	}()

	// Stream these blocks and wait until we are done.
	bs := adder.NewBlockStreamer(ctx, dgs.rpcClient, []peer.ID{""}, blocks)
	select {
	case <-ctx.Done():
		return api.CidUndef, ctx.Err()
	case <-bs.Done():
	}

	if err := bs.Err(); err != nil {
		return dataRoot, err
	}

	clusterDAG := clusterDAGNodes[0].Cid()

	dgs.sendOutput(api.AddedOutput{
		Name:        fmt.Sprintf("%s-clusterDAG", dgs.addParams.Name),
		Cid:         api.NewCid(clusterDAG),
		Size:        dgs.totalSize,
		Allocations: nil,
	})

	// Pin the ClusterDAG
	clusterDAGPin := api.PinWithOpts(api.NewCid(clusterDAG), dgs.addParams.PinOptions)
	clusterDAGPin.ReplicationFactorMin = -1
	clusterDAGPin.ReplicationFactorMax = -1
	clusterDAGPin.MaxDepth = 0 // pin direct
	clusterDAGPin.Name = fmt.Sprintf("%s-clusterDAG", dgs.addParams.Name)
	clusterDAGPin.Type = api.ClusterDAGType
	clusterDAGPin.Reference = &dataRoot
	dataShardSize := dgs.rs.GetDataShardSize()
	// record data shard size to metadata, enable erasure module know data size and the number.
	for i, size := range dataShardSize {
		logger.Debug(i, size)
		clusterDAGPin.Metadata[i] = fmt.Sprintf("%d", size)
	}
	for _, bmeta := range dgs.blockMeta {
		logger.Debug(bmeta)
		clusterDAGPin.Metadata[bmeta.String()] = ""
	}
	// Update object with response.
	err = adder.Pin(ctx, dgs.rpcClient, clusterDAGPin)
	if err != nil {
		return dataRoot, err
	}

	// Pin the META pin
	metaPin := api.PinWithOpts(dataRoot, dgs.addParams.PinOptions)
	metaPin.Type = api.MetaType
	ref := api.NewCid(clusterDAG)
	metaPin.Reference = &ref
	metaPin.MaxDepth = 0 // irrelevant. Meta-pins are not pinned
	metaPin.Metadata = nil
	err = adder.Pin(ctx, dgs.rpcClient, metaPin)
	if err != nil {
		return dataRoot, err
	}

	// Log some stats
	dgs.logStats(metaPin.Cid, clusterDAGPin.Cid)

	return dataRoot, nil
}

// Ingest block for Raptor coding and sharding.
func (dgs *DAGService) ingestBlock(ctx context.Context, n ipld.Node) error {
	shard := dgs.currentShard

	if shard == nil {
		logger.Infof("new shard for '%s': #%d", dgs.addParams.Name, len(dgs.shards))
		var err error
		shard, err = newShard(dgs.ctx, ctx, dgs.rpcClient, dgs.addParams.PinOptions, len(dgs.shards), dgs.addParams.Erasure)
		if err != nil {
			return err
		}
		dgs.currentShard = shard
	}

	size := uint64(len(n.RawData()))
	if shard.Size()+size < shard.Limit() {
		if dgs.addParams.Erasure {
			dgs.rs.AddBlock(n.RawData(), len(dgs.shards))
		}
		shard.AddLink(ctx, n.Cid(), size)
		return dgs.currentShard.sendBlock(ctx, n)
	}

	if shard.Size() == 0 {
		return errors.New("block doesn't fit in empty shard: shard size too small?")
	}

	_, err := dgs.FlushCurrentShard(ctx)
	if err != nil {
		return err
	}
	return dgs.ingestBlock(ctx, n)
}

func (dgs *DAGService) FlushCurrentShard(ctx context.Context) (api.Cid, error) {
	shard := dgs.currentShard
	if shard == nil {
		return api.CidUndef, errors.New("cannot flush a nil shard")
	}

	shardCid, err := shard.Flush(ctx, len(dgs.shards), dgs.previousShard)
	if err != nil {
		return api.NewCid(shardCid), err
	}

	if dgs.addParams.Erasure {
		dgs.rs.FinalizeShard(len(dgs.shards))
	}

	dgs.totalSize += shard.Size()
	dgs.shards[fmt.Sprintf("%d", len(dgs.shards))] = shardCid
	for _, bmeta := range shard.blockMeta {
		dgs.blockMeta = append(dgs.blockMeta, bmeta)
	}
	dgs.previousShard = shardCid
	dgs.currentShard = nil

	dgs.sendOutput(api.AddedOutput{
		Name:        fmt.Sprintf("shard-%d", len(dgs.shards)),
		Cid:         api.NewCid(shardCid),
		Size:        shard.Size(),
		Allocations: shard.Allocations(),
	})

	return api.NewCid(shard.LastLink()), nil
}

// Log stats for the sharding process
func (dgs *DAGService) logStats(metaPin, clusterDAGPin api.Cid) {
	duration := time.Since(dgs.startTime)
	seconds := uint64(duration) / uint64(time.Second)
	var rate string
	if seconds == 0 {
		rate = "∞ B"
	} else {
		rate = humanize.Bytes(dgs.totalSize / seconds)
	}

	statsFmt := `sharding session successful:
CID: %s
Name: %s
ClusterDAG: %s
Total shards: %d
Total size: %d
Total time: %s
Ingest Rate: %s/s
`

	logger.Infof(
		statsFmt,
		metaPin,
		dgs.addParams.Name,
		clusterDAGPin,
		len(dgs.shards),
		dgs.totalSize,
		duration,
		rate,
	)
}

// AddMany calls Add for every node in the list
func (dgs *DAGService) AddMany(ctx context.Context, nodes []ipld.Node) error {
	for _, node := range nodes {
		err := dgs.Add(ctx, node)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetRS returns the Raptor object
func (dgs *DAGService) GetRS() *ec.Raptor {
	return dgs.rs
}

// SetParity is a placeholder for setting parity in Raptor coding
func (dgs *DAGService) SetParity(name string) {
	// Implementation for setting parity information if needed
}

