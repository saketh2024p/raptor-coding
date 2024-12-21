// Package erasure implements erasure coding for IPFS Cluster.
package erasure

import (
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	raptor "github.com/example/raptor-codes" // Placeholder for Raptor coding library
)

var log = logging.Logger("erasure")

type BlockStat string

const (
	DefaultBlock  = BlockStat("defaultBlock")
	ShardEndBlock = BlockStat("shardEndBlock")
	FileEndBlock  = BlockStat("fileEndBlock")
)

type StatBlock struct {
	ipld.Node
	Stat BlockStat
	api.Cid
}

type Shard struct {
	api.Cid
	Idx     int
	Name    string
	RawData []byte
	Links   map[int]api.Cid // Links to blocks
}

type Batch struct {
	Idx       int
	NeedRepin []int // Shards[i] broken, need to repin
	Shards    [][]byte
}

type Raptor struct {
	mu            sync.Mutex
	raptorEncoder raptor.Encoder
	ctx           context.Context
	blocks        [][]byte
	batchCid      map[int]api.Cid
	dataShardSize map[string]uint64
	parityCids    map[string]cid.Cid // Send to sharding dag service
	dataShards    int
	parityShards  int
	curShardI     int
	curShardJ     int
	shardSize     int
	parityLen     int
	dataLen       int
	parityCh      chan Shard
	blockCh       chan StatBlock
	receAllData   bool
}

// New initializes the Raptor encoder.
func New(ctx context.Context, d int, p int, shardSize int) *Raptor {
	raptorEncoder := raptor.NewEncoder(d, p) // Replace Reed-Solomon with Raptor
	r := &Raptor{
		mu:            sync.Mutex{},
		raptorEncoder: raptorEncoder,
		ctx:           ctx,
		blocks:        newBlocks(d, p, shardSize),
		dataShardSize: make(map[string]uint64),
		batchCid:      make(map[int]api.Cid),
		parityCids:    make(map[string]cid.Cid),
		shardSize:     shardSize,
		dataShards:    d,
		parityShards:  p,
		curShardI:     0,
		curShardJ:     0,
		parityLen:     0,
		dataLen:       0,
		parityCh:      make(chan Shard, 1024),
		blockCh:       make(chan StatBlock, 1024),
		receAllData:   false,
	}
	if shardSize != 0 {
		go r.handleBlock()
	}
	return r
}

// handleBlock processes blocks from dag_service.
func (r *Raptor) handleBlock() {
	for {
		blk := <-r.blockCh
		r.mu.Lock()
		switch blk.Stat {
		case DefaultBlock:
			b := blk.RawData()
			if r.curShardJ+len(b) <= r.shardSize {
				r.blocks[r.curShardI] = append(r.blocks[r.curShardI], b...)
				r.curShardJ += len(b)
			} else {
				log.Errorf("unreachable code, block size:%d, curShardJ:%d, shardSize:%d", len(b), r.curShardJ, r.shardSize)
			}
		case ShardEndBlock:
			r.dataShardSize[fmt.Sprintf("%d", r.dataLen)] = uint64(len(r.blocks[r.curShardI]))
			r.dataLen++
			r.batchCid[r.curShardI] = blk.Cid
			r.curShardI, r.curShardJ = r.curShardI+1, 0
			if r.curShardI == r.dataShards {
				r.EncodeL(false)
			}
		case FileEndBlock:
			r.EncodeL(true)
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
	}
}

// EncodeL encodes data shards into parity shards.
func (r *Raptor) EncodeL(isLast bool) {
	if isLast && r.curShardI == 0 && r.curShardJ == 0 {
		close(r.parityCh)
		r.receAllData = isLast
		return
	}
	r.alignL()
	parityShards, err := r.raptorEncoder.Encode(r.blocks[:r.dataShards])
	if err != nil {
		log.Errorf("raptor encode error:%v", err)
		return
	}
	for _, b := range parityShards {
		r.parityCh <- Shard{
			RawData: b,
			Name:    fmt.Sprintf("parity-shard-%d", r.parityLen),
			Links:   r.batchCid,
		}
		r.parityLen++
	}
	r.blocks = newBlocks(r.dataShards, r.parityShards, r.shardSize)
	if isLast {
		close(r.parityCh)
		r.receAllData = isLast
	}
}

func (r *Raptor) alignL() {
	eachShard := 0
	for i := 0; i < r.curShardI; i++ {
		eachShard = max(len(r.blocks[i]), eachShard)
	}
	for i := 0; i < r.curShardI; i++ {
		r.blocks[i] = append(r.blocks[i], make([]byte, eachShard-len(r.blocks[i]))...)
	}
	for r.curShardI < (r.dataShards + r.parityShards) {
		r.blocks[r.curShardI] = append(r.blocks[r.curShardI], make([]byte, eachShard)...)
		r.curShardI++
	}
	r.curShardI, r.curShardJ = 0, 0
}

func newBlocks(d, p, shardSize int) [][]byte {
	b := make([][]byte, d+p)
	for i := range b[:d] {
		b[i] = make([]byte, 0, shardSize)
	}
	return b
}

func (r *Raptor) ReceiveAll() bool {
	return r.receAllData
}

func (r *Raptor) GetParityShards() map[string]cid.Cid {
	return r.parityCids
}

func (r *Raptor) AddParityCid(key int, cid api.Cid) {
	r.parityCids[fmt.Sprintf("%d", key)] = cid.Cid
}

func (r *Raptor) GetDataShardSize() map[string]uint64 {
	return r.dataShardSize
}

func (r *Raptor) GetParity() <-chan Shard {
	return r.parityCh
}

func (r *Raptor) SendBlockTo() chan<- StatBlock {
	return r.blockCh
}

// BatchRecon reconstructs missing shards in a batch.
func (r *Raptor) BatchRecon(ctx context.Context, batchIdx int, batchDataShardSize []int, shardCh <-chan Shard) (error, Batch) {
	batch := make([][]byte, r.dataShards+r.parityShards)
	received := 0
	for {
		select {
		case <-ctx.Done():
			log.Error(ctx.Err())
			return fmt.Errorf("%d batch err:%s", batchIdx, ctx.Err()), Batch{}
		case shard := <-shardCh:
			if len(shard.RawData) == 0 {
				log.Infof("BatchRecon received invalid shard")
				continue
			}
			batch[shard.Idx] = shard.RawData
			received++
			if received >= len(batchDataShardSize) {
				reconstructed, err := r.raptorEncoder.Reconstruct(batch)
				if err != nil {
					log.Errorf("reconstruct error: %v", err)
					return err, Batch{}
				}
				return nil, Batch{Idx: batchIdx, Shards: reconstructed}
			}
		}
	}
}
