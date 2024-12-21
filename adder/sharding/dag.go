// Package sharding implements functions for constructing and parsing ipld-cbor nodes
// of the clusterDAG used to track sharded DAGs in ipfs-cluster.
package sharding

import (
	"context"
	"fmt"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	ipld "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
)

// MaxLinks defines the maximum number of links that fit into a block.
const MaxLinks = 5984
const hashFn = mh.SHA2_256

// CborDataToNode parses cbor data into a clusterDAG node while making a few checks.
func CborDataToNode(raw []byte, format string) (ipld.Node, error) {
	if format != "cbor" {
		return nil, fmt.Errorf("unexpected shard node format %s", format)
	}
	shardCid, err := cid.NewPrefixV1(cid.DagCBOR, hashFn).Sum(raw)
	if err != nil {
		return nil, err
	}
	shardBlk, err := blocks.NewBlockWithCid(raw, shardCid)
	if err != nil {
		return nil, err
	}
	shardNode, err := cbor.DecodeBlock(shardBlk)
	if err != nil {
		return nil, err
	}
	return shardNode, nil
}

// makeDAGSimple creates a single-layer DAG node for a given object.
func makeDAGSimple(ctx context.Context, dagObj map[string]cid.Cid) (ipld.Node, error) {
	node, err := cbor.WrapObject(
		dagObj,
		hashFn, mh.DefaultLengths[hashFn],
	)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// MakeDAG constructs a DAG for the given object, handling cases where links exceed block size.
func MakeDAG(ctx context.Context, dagObj map[string]cid.Cid) ([]ipld.Node, error) {
	if len(dagObj) <= MaxLinks {
		n, err := makeDAGSimple(ctx, dagObj)
		return []ipld.Node{n}, err
	}

	leafNodes := make([]ipld.Node, 0)
	indirectObj := make(map[string]cid.Cid)
	numFullLeaves := len(dagObj) / MaxLinks

	for i := 0; i <= numFullLeaves; i++ {
		leafObj := make(map[string]cid.Cid)
		for j := 0; j < MaxLinks; j++ {
			c, ok := dagObj[fmt.Sprintf("%d", i*MaxLinks+j)]
			if !ok {
				if i != numFullLeaves {
					return nil, fmt.Errorf("unexpected state: incomplete leaf before end")
				}
				break
			}
			leafObj[fmt.Sprintf("%d", j)] = c
		}
		leafNode, err := makeDAGSimple(ctx, leafObj)
		if err != nil {
			return nil, err
		}
		indirectObj[fmt.Sprintf("%d", i)] = leafNode.Cid()
		leafNodes = append(leafNodes, leafNode)
	}

	indirectNode, err := makeDAGSimple(ctx, indirectObj)
	if err != nil {
		return nil, err
	}

	nodes := append([]ipld.Node{indirectNode}, leafNodes...)
	return nodes, nil
}

// Optimize and simplify error handling and modularity to facilitate future scalability.
