package adder

import (
	"context"
	"errors"
	"hash/fnv"
	"sort"
	"sync"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"go.uber.org/multierr"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

// ErrBlockAdder is returned when adding to multiple destinations
// block fails on all of them.
var ErrBlockAdder = errors.New("failed to put block on all destinations")

// BlockStreamer helps streaming nodes to multiple destinations, as long as
// one of them is still working.
type BlockStreamer struct {
	dests     []peer.ID
	rpcClient *rpc.Client
	blocks    <-chan api.NodeWithMeta

	ctx    context.Context
	cancel context.CancelFunc
	errMu  sync.Mutex
	err    error
}

// NewBlockStreamer creates a BlockStreamer given an RPC client, allocated
// peers, and a channel on which the blocks to stream are received.
func NewBlockStreamer(ctx context.Context, rpcClient *rpc.Client, dests []peer.ID, blocks <-chan api.NodeWithMeta) *BlockStreamer {
	bsCtx, cancel := context.WithCancel(ctx)

	bs := BlockStreamer{
		ctx:       bsCtx,
		cancel:    cancel,
		dests:     dests,
		rpcClient: rpcClient,
		blocks:    blocks,
		err:       nil,
	}

	go bs.streamBlocks()
	return &bs
}

// Done returns a channel that gets closed when the BlockStreamer has
// finished.
func (bs *BlockStreamer) Done() <-chan struct{} {
	return bs.ctx.Done()
}

func (bs *BlockStreamer) setErr(err error) {
	bs.errMu.Lock()
	bs.err = err
	bs.errMu.Unlock()
}

// Err returns any errors that occurred during the operation of the
// BlockStreamer, for example when blocks could not be put to all nodes.
func (bs *BlockStreamer) Err() error {
	bs.errMu.Lock()
	defer bs.errMu.Unlock()
	return bs.err
}

func (bs *BlockStreamer) streamBlocks() {
	defer bs.cancel()

	// Nothing should be sent on out.
	// We drain though.
	out := make(chan struct{})
	go func() {
		for range out {
		}
	}()

	errs := bs.rpcClient.MultiStream(
		bs.ctx,
		bs.dests,
		"IPFSConnector",
		"BlockStream",
		bs.blocks,
		out,
	)

	combinedErrors := multierr.Combine(errs...)

	// FIXME: replicate everywhere.
	if len(multierr.Errors(combinedErrors)) == len(bs.dests) {
		logger.Error(combinedErrors)
		bs.setErr(ErrBlockAdder)
	} else if combinedErrors != nil {
		logger.Warning("there were errors streaming blocks, but at least one destination succeeded")
		logger.Warning(combinedErrors)
	}
}

// IpldNodeToNodeWithMeta converts an IPLD node to an `api.NodeWithMeta`.
func IpldNodeToNodeWithMeta(n ipld.Node) api.NodeWithMeta {
	size, err := n.Size()
	if err != nil {
		logger.Warn(err)
	}

	return api.NodeWithMeta{
		Cid:     api.NewCid(n.Cid()),
		Data:    n.RawData(),
		CumSize: size,
	}
}

// BlockAllocate helps allocate blocks to peers.
func BlockAllocate(ctx context.Context, rpc *rpc.Client, pinOpts api.PinOptions) ([]peer.ID, error) {
	// Find where to allocate this file
	var allocsStr []peer.ID
	err := rpc.CallContext(
		ctx,
		"",
		"Cluster",
		"BlockAllocate",
		api.PinWithOpts(api.CidUndef, pinOpts),
		&allocsStr,
	)
	return allocsStr, err
}

// DefaultECAllocate selects a peer to send a shard.
// It uses a round-robin strategy to allocate shards to peers, leveraging hash-based sorting and distributing shards between data and parity peers.
func DefaultECAllocate(ctx context.Context, rpc *rpc.Client, hashName string, d, p, idx int, isData bool) (peer.ID, error) {
	if d <= 0 || p <= 0 || idx < 0 {
		return "", errors.New("invalid input")
	}

	var peers []peer.ID
	err := rpc.CallContext(
		ctx,
		"",
		"Consensus",
		"Peers",
		struct{}{},
		&peers,
	)
	if err != nil {
		return "", err
	}

	if len(peers) == 0 {
		return "", errors.New("no peers available")
	}

	if len(peers) == 1 {
		return peers[0], nil
	}

	// Sort peers by hash.
	h := fnv.New32()
	hashPeers := make(map[peer.ID]uint32)
	for _, peer := range peers {
		h.Reset()
		h.Write([]byte(hashName + peer.String()))
		hashPeers[peer] = h.Sum32()
	}
	sort.Slice(peers, func(i, j int) bool {
		return hashPeers[peers[i]] < hashPeers[peers[j]]
	})

	// Divide peers into data and parity groups.
	dNum := d * len(peers) / (d + p)
	if d*len(peers)%(d+p) != 0 && dNum < len(peers)-1 {
		dNum++
	}
	dPeers := peers[:dNum]
	pPeers := peers[dNum:]

	if isData {
		return dPeers[idx%len(dPeers)], nil
	}
	return pPeers[idx%len(pPeers)], nil
}

// ErasurePin sends local and remote RPC pin requests by enabling permissions for RPC calls.
func ErasurePin(ctx context.Context, rpc *rpc.Client, pin api.Pin) error {
	logger.Debugf("adder pinning %+v", pin)
	var pinResp api.Pin
	return rpc.CallContext(
		ctx,
		pin.Allocations[0],
		"Cluster",
		"Pin",
		pin,
		&pinResp,
	)
}

// Pin sends local RPC pin requests.
func Pin(ctx context.Context, rpc *rpc.Client, pin api.Pin) error {
	if pin.ReplicationFactorMin < 0 {
		pin.Allocations = []peer.ID{}
	}
	logger.Debugf("adder pinning %+v", pin)
	var pinResp api.Pin
	return rpc.CallContext(
		ctx,
		"",
		"Cluster",
		"Pin",
		pin,
		&pinResp,
	)
}
