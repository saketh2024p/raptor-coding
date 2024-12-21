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

// DAGService is an implementation of an adder.ClusterDAGService that
// directly adds blocks without sharding. It supports Raptor coding for redundancy.
type DAGService struct {
	adder.BaseDAGService

	ctx             context.Context
	rpcClient       *rpc.Client
	dests           []peer.ID
	addParams       api.AddParams
	local           bool
	bs              *adder.BlockStreamer
	recentBlocks    *recentBlocks
	blocks          []chan api.NodeWithMeta
	closeBlocksOnce []sync.Once
	fileIdx         int
}

// New returns a new Adder with the given rpc Client. The client is used
// to perform calls to IPFS.BlockStream and Pin content on Cluster.
func New(ctx context.Context, rpc *rpc.Client, opts api.AddParams, local bool) *DAGService {
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
	if dgs.recentBlocks.Has(node) {
		return nil
	}

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
				dests[i] = localPid
			}
		}

		dgs.blocks = append(dgs.blocks, make(chan api.NodeWithMeta, 256))
		dgs.closeBlocksOnce = append(dgs.closeBlocksOnce, sync.Once{})
		if dgs.addParams.Erasure {
			fileName := dgs.extractFileName()
			allocation, err := adder.DefaultECAllocate(ctx, dgs.rpcClient, fileName, dgs.addParams.DataShards, dgs.addParams.ParityShards, dgs.fileIdx, false)
			if err != nil {
				return fmt.Errorf("parity shard allocation %d: %s", dgs.fileIdx, err)
			}
			dests = []peer.ID{allocation}
		}
		dgs.dests = dests

		if dgs.local {
			if !hasLocal && localPid != "" && len(dgs.addParams.UserAllocations) == 0 {
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

// Finalize pins the last Cid added to this DAGService.
func (dgs *DAGService) Finalize(ctx context.Context, root api.Cid) (api.Cid, error) {
	dgs.CloseI(dgs.fileIdx)
	select {
	case <-dgs.ctx.Done():
		return root, ctx.Err()
	case <-ctx.Done():
		return root, ctx.Err()
	case <-dgs.bs.Done():
	}

	if err := dgs.bs.Err(); err != nil {
		return root, err
	}

	if dgs.addParams.NoPin {
		return root, nil
	}

	rootPin := api.PinWithOpts(root, dgs.addParams.PinOptions)
	rootPin.Allocations = append(rootPin.Allocations, dgs.dests...)
	dgs.dests = nil
	if dgs.addParams.Erasure {
		rootPin.ReplicationFactorMax = 1
		rootPin.ReplicationFactorMin = 1
		return root, adder.RaptorPin(ctx, dgs.rpcClient, rootPin)
	} else {
		return root, adder.Pin(ctx, dgs.rpcClient, rootPin)
	}
}

// ExtractFileName removes Raptor parity shard identifiers from the file name.
func (dgs *DAGService) extractFileName() string {
	fileName := dgs.addParams.Name
	for i := 0; i < 3; i++ {
		fileName = fileName[:strings.LastIndex(fileName, "-")]
	}
	return fileName
}

// Additional unchanged methods like `Close`, `CloseI`, etc. remain here.
