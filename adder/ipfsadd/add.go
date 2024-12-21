// Package ipfsadd is a simplified copy of go-ipfs/core/coreunix/add.go
package ipfsadd

import (
	"context"
	"errors"
	"fmt"
	"io"
	gopath "path"
	"path/filepath"

	"github.com/ipfs-cluster/ipfs-cluster/api"

	chunker "github.com/ipfs/boxo/chunker"
	files "github.com/ipfs/boxo/files"
	posinfo "github.com/ipfs/boxo/filestore/posinfo"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	unixfs "github.com/ipfs/boxo/ipld/unixfs"
	balanced "github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	ihelper "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	trickle "github.com/ipfs/boxo/ipld/unixfs/importer/trickle"
	mfs "github.com/ipfs/boxo/mfs"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	peer "github.com/libp2p/go-libp2p/core/peer"
	raptor "github.com/example/raptor-codes" // Placeholder for Raptor coding library
)

var log = logging.Logger("coreunix")

// NewAdder returns a new Adder used for a file add operation.
func NewAdder(ctx context.Context, ds ipld.DAGService, allocs func() []peer.ID) (*Adder, error) {
	return &Adder{
		ctx:        ctx,
		dagService: ds,
		allocsFun:  allocs,
		Progress:   false,
		Trickle:    false,
		Chunker:    "",
	}, nil
}

// Adder holds the switches passed to the `add` command.
type Adder struct {
	ctx        context.Context
	dagService ipld.DAGService
	allocsFun  func() []peer.ID
	Out        chan api.AddedOutput
	Progress   bool
	Trickle    bool
	RawLeaves  bool
	Silent     bool
	NoCopy     bool
	Chunker    string
	CidBuilder cid.Builder
	liveNodes  uint64
	OutputPrefix string
}

// Constructs a node from reader's data, and adds it. Doesn't pin.
func (adder *Adder) add(reader io.Reader) (ipld.Node, error) {
	chnk, err := chunker.FromString(reader, adder.Chunker)
	if err != nil {
		return nil, err
	}

	params := ihelper.DagBuilderParams{
		Dagserv:    adder.dagService,
		RawLeaves:  adder.RawLeaves,
		Maxlinks:   ihelper.DefaultLinksPerBlock,
		NoCopy:     adder.NoCopy,
		CidBuilder: adder.CidBuilder,
	}

	db, err := params.New(chnk)
	if err != nil {
		return nil, err
	}

	var nd ipld.Node
	if adder.Trickle {
		nd, err = trickle.Layout(db)
	} else {
		nd, err = balanced.Layout(db)
	}
	if err != nil {
		return nil, err
	}

	// Raptor encoding integration: Process the node for parity shards.
	err = adder.processRaptorEncoding(nd)
	if err != nil {
		return nil, fmt.Errorf("raptor encoding error: %v", err)
	}

	return nd, nil
}

// processRaptorEncoding generates parity shards using Raptor codes.
func (adder *Adder) processRaptorEncoding(node ipld.Node) error {
	data := node.RawData()
	raptorEncoder := raptor.NewEncoder(4, 2) // Example: 4 data shards, 2 parity shards

	parityShards, err := raptorEncoder.Encode(data)
	if err != nil {
		return fmt.Errorf("failed to generate parity shards: %v", err)
	}

	// Add parity shards to the DAG service.
	for _, shard := range parityShards {
		parityNode := dag.NodeWithData(shard)
		err := adder.dagService.Add(adder.ctx, parityNode)
		if err != nil {
			return fmt.Errorf("failed to add parity shard: %v", err)
		}
	}
	return nil
}

// AddAllAndPin adds the given request's files and pin them.
func (adder *Adder) AddAllAndPin(file files.Node) (ipld.Node, error) {
	if err := adder.addFileNode("", file, true); err != nil {
		return nil, err
	}

	mr, err := adder.mfsRoot()
	if err != nil {
		return nil, err
	}

	root := mr.GetDirectory()
	err = root.Flush()
	if err != nil {
		return nil, err
	}

	nd, err := root.GetNode()
	if err != nil {
		return nil, err
	}

	// Cluster: call PinRoot which adds the root CID to the DAGService.
	return nd, adder.PinRoot(nd)
}
