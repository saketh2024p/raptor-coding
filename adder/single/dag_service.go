// Package single implements a ClusterDAGService that chunks and adds content
// to cluster without sharding, before pinning it.
package single

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	adder "github.com/ipfs-cluster/ipfs-cluster/adder"
	ec "github.com/ipfs-cluster/ipfs-cluster/adder/erasure"
	"github.com/ipfs-cluster/ipfs-cluster/api"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

var logger = logging.Logger("singledags")
var _ = logger // otherwise unused

// DAGService is an implementation of an adder.ClusterDAGService which
// puts the added blocks directly in the peers allocated to them (without
// sharding).
type DAGService struct {
	adder.BaseDAGService

	ctx       context.Context
	rpcClient *rpc.Client

	dests     []peer.ID
	addParams api.AddParams
	local     bool

	bs           *adder.BlockStreamer
	recentBlocks *recentBlocks
	// erasure need to add serveral parity shards
	blocks          []chan api.NodeWithMeta
	closeBlocksOnce []sync.Once
	fileIdx         int // parity shard index, only used when enable erasure code
}

// New returns a new Adder with the given rpc Client. The client is used
// to perform calls to IPFS.BlockStream and Pin content on Cluster.
func New(ctx context.Context, rpc *rpc.Client, opts api.AddParams, local bool) *DAGService {
	// ensure don't Add something and pin it in direct mode.
	opts.Mode = api.PinModeRecursive
	return &DAGService{
		ctx:             ctx,
		rpcClient:       rpc,
		dests:           nil,
		addParams:       opts,
		local:           local,
		blocks:          make([]chan api.NodeWithMeta, 0),
		closeBlocksOnce: make([]sync.Once, 0),
		recentBlocks:    &recentBlocks{},
	}
}

// Add puts the given node in the destination peers.
func (dgs *DAGService) Add(ctx context.Context, node ipld.Node) error {
	// Avoid adding the same node multiple times in a row.
	// This is done by the ipfsadd-er, because some nodes are added
	// via dagbuilder, then via MFS, and root nodes once more.
	if dgs.recentBlocks.Has(node) {
		return nil
	}

	// FIXME: can't this happen on initialization?  Perhaps the point here
	// is the adder only allocates and starts streaming when the first
	// block arrives and not on creation.
	if dgs.dests == nil {
		dests, err := adder.BlockAllocate(ctx, dgs.rpcClient, dgs.addParams.PinOptions)
		if err != nil {
			return err
		}

		hasLocal := false
		localPid := dgs.rpcClient.ID()
		for i, d := range dests {
			if d == localPid || d == "" {
				hasLocal = true
				// ensure our allocs do not carry an empty peer
				// mostly an issue with testing mocks
				dests[i] = localPid
			}
		}

		dgs.blocks = append(dgs.blocks, make(chan api.NodeWithMeta, 256))
		dgs.closeBlocksOnce = append(dgs.closeBlocksOnce, sync.Once{})
		if dgs.addParams.Erasure {
			// sets allocations to one peer
			// parse fileName-parity-shard-x to fileName
			fileName := dgs.addParams.Name[:strings.LastIndex(dgs.addParams.Name, "-")]
			fileName = fileName[:strings.LastIndex(fileName, "-")]
			fileName = fileName[:strings.LastIndex(fileName, "-")]
			allocation, err := adder.DefaultECAllocate(ctx, dgs.rpcClient, fileName, dgs.addParams.DataShards, dgs.addParams.ParityShards, dgs.fileIdx, false)
			if err != nil {
				return fmt.Errorf("parity shard allocation %d: %s", dgs.fileIdx, err)
			}
			dests = []peer.ID{allocation}
		}
		dgs.dests = dests
		if dgs.local {
			// If this is a local pin, make sure that the local
			// peer is among the allocations..
			// UNLESS user-allocations are defined!
			if !hasLocal && localPid != "" && len(dgs.addParams.UserAllocations) == 0 {
				// replace last allocation with local peer
				dgs.dests[len(dgs.dests)-1] = localPid
			}

			dgs.bs = adder.NewBlockStreamer(dgs.ctx, dgs.rpcClient, []peer.ID{localPid}, dgs.blocks[dgs.fileIdx])
		} else {
			dgs.bs = adder.NewBlockStreamer(dgs.ctx, dgs.rpcClient, dgs.dests, dgs.blocks[dgs.fileIdx])
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-dgs.ctx.Done():
		return ctx.Err()
	case dgs.blocks[dgs.fileIdx] <- adder.IpldNodeToNodeWithMeta(node):
		dgs.recentBlocks.Add(node)
		return nil
	}
}

// Close cleans up the DAGService.
func (dgs *DAGService) Close() error {
	for i := range dgs.closeBlocksOnce {
		dgs.closeBlocksOnce[i].Do(func() {
			close(dgs.blocks[i])
		})
	}
	return nil
}

// Close i blockstream.
func (dgs *DAGService) CloseI(i int) {
	dgs.closeBlocksOnce[i].Do(func() {
		close(dgs.blocks[i])
	})
}

// Finalize pins the last Cid added to this DAGService.
func (dgs *DAGService) Finalize(ctx context.Context, root api.Cid) (api.Cid, error) {
	// Close current blocks channel
	dgs.CloseI(dgs.fileIdx)
	// Wait for the BlockStreamer to finish.
	select {
	case <-dgs.ctx.Done():
		return root, ctx.Err()
	case <-ctx.Done():
		return root, ctx.Err()
	case <-dgs.bs.Done():
	}

	// If the streamer failed to put blocks.
	if err := dgs.bs.Err(); err != nil {
		return root, err
	}

	// Do not pin, just block put.
	// Why? Because some people are uploading CAR files with partial DAGs
	// and ideally they should be pinning only when the last partial CAR
	// is uploaded. This gives them that option.
	if dgs.addParams.NoPin {
		return root, nil
	}
	// Cluster pin the result
	rootPin := api.PinWithOpts(root, dgs.addParams.PinOptions)
	rootPin.Allocations = append(rootPin.Allocations, dgs.dests...)
	dgs.dests = nil
	if dgs.addParams.Erasure {
		rootPin.ReplicationFactorMax = 1
		rootPin.ReplicationFactorMin = 1
		return root, adder.ErasurePin(ctx, dgs.rpcClient, rootPin)
	} else {
		return root, adder.Pin(ctx, dgs.rpcClient, rootPin)
	}
}

// Allocations returns the add destinations decided by the DAGService.
func (dgs *DAGService) Allocations() []peer.ID {
	// using rpc clients without a host results in an empty peer
	// which cannot be parsed to peer.ID on deserialization.
	if len(dgs.dests) == 1 && dgs.dests[0] == "" {
		return nil
	}
	return dgs.dests
}

// AddMany calls Add for every given node.
func (dgs *DAGService) AddMany(ctx context.Context, nodes []ipld.Node) error {
	for _, node := range nodes {
		err := dgs.Add(ctx, node)
		if err != nil {
			return err
		}
	}
	return nil
}

type recentBlocks struct {
	blocks [2]cid.Cid
	cur    int
}

func (rc *recentBlocks) Add(n ipld.Node) {
	rc.blocks[rc.cur] = n.Cid()
	rc.cur = (rc.cur + 1) % 2
}

func (rc *recentBlocks) Has(n ipld.Node) bool {
	c := n.Cid()
	return rc.blocks[0].Equals(c) || rc.blocks[1].Equals(c)
}

func (dgs *DAGService) GetRS() *ec.ReedSolomon {
	return nil // never use single dag_service to get rs
}

func (dgs *DAGService) SetParity(name string) {
	nameSplit := strings.Split(name, "-")
	idx, _ := strconv.Atoi(nameSplit[len(nameSplit)-1]) // get parity index
	dgs.addParams.Erasure = true
	dgs.addParams.Name = name
	dgs.fileIdx = idx
}

func (dgs *DAGService) FlushCurrentShard(ctx context.Context) (cid api.Cid, err error) {
	return api.CidUndef, err
}
