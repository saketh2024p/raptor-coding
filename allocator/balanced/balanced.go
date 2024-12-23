// Package balanced implements an allocator that can sort allocations
// based on multiple metrics, where metrics may be an arbitrary way to
// partition a set of peers.
//
// For example, allocating by ["tag:region", "disk"] the resulting peer
// candidate order will balance between regions and order by the value of
// the weight of the disk metric.
package balanced

import (
	"context"
	"fmt"
	"sort"

	api "github.com/ipfs-cluster/ipfs-cluster/api"
	logging "github.com/ipfs/go-log/v2"
	peer "github.com/libp2p/go-libp2p/core/peer"
	rpc "github.com/libp2p/go-libp2p-gorpc"
)

var logger = logging.Logger("allocator")

// Allocator is an allocator that partitions metrics and orders
// the final list of allocations by selecting for each partition.
type Allocator struct {
	config    *Config
	rpcClient *rpc.Client
}

// New returns an initialized Allocator.
func New(cfg *Config) (*Allocator, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	return &Allocator{
		config: cfg,
	}, nil
}

// SetClient provides an rpc.Client, allowing the allocator to contact
// other components in the cluster.
func (a *Allocator) SetClient(c *rpc.Client) {
	a.rpcClient = c
}

// Shutdown invalidates metrics and cleans up resources during cluster shutdown.
func (a *Allocator) Shutdown(ctx context.Context) error {
	a.rpcClient = nil
	return nil
}

// partitionedMetric represents metrics partitioned by specific values.
type partitionedMetric struct {
	metricName       string
	curChoosingIndex int
	noMore           bool
	partitions       []*partition
}

type partition struct {
	value            string
	weight           int64
	aggregatedWeight int64
	peers            map[peer.ID]bool
	sub              *partitionedMetric
}

// partitionMetrics recursively partitions metrics by given criteria.
func partitionMetrics(set api.MetricsSet, by []string) *partitionedMetric {
	rootMetric := by[0]
	pnedMetric := &partitionedMetric{
		metricName: rootMetric,
		partitions: partitionValues(set[rootMetric]),
	}

	// Sorting partitions by weight and other criteria.
	lessF := func(i, j int) bool {
		wi := pnedMetric.partitions[i].weight
		wj := pnedMetric.partitions[j].weight
		if wi == wj {
			awi := pnedMetric.partitions[i].aggregatedWeight
			awj := pnedMetric.partitions[j].aggregatedWeight
			if awi == awj {
				return pnedMetric.partitions[i].value < pnedMetric.partitions[j].value
			}
			return awj < awi
		}
		return wj < wi
	}

	if len(by) == 1 {
		sort.Slice(pnedMetric.partitions, lessF)
		return pnedMetric
	}

	// Process sub-partitions.
	for _, partition := range pnedMetric.partitions {
		filteredSet := make(api.MetricsSet)
		for k, v := range set {
			if k == rootMetric {
				continue
			}
			for _, m := range v {
				if _, ok := partition.peers[m.Peer]; ok {
					filteredSet[k] = append(filteredSet[k], m)
				}
			}
		}

		partition.sub = partitionMetrics(filteredSet, by[1:])

		// Aggregate subpartition weights.
		for _, subp := range partition.sub.partitions {
			partition.aggregatedWeight += subp.aggregatedWeight
		}
	}
	sort.Slice(pnedMetric.partitions, lessF)
	return pnedMetric
}

// partitionValues groups peers by their metric values and weights.
func partitionValues(metrics []api.Metric) []*partition {
	partitions := []*partition{}

	if len(metrics) == 0 {
		return partitions
	}

	partitionsByValue := make(map[string]*partition)
	for _, m := range metrics {
		if !m.Partitionable {
			partitions = append(partitions, &partition{
				value:            m.Value,
				weight:           m.GetWeight(),
				aggregatedWeight: m.GetWeight(),
				peers: map[peer.ID]bool{
					m.Peer: false,
				},
			})
			continue
		}

		if p, ok := partitionsByValue[m.Value]; ok {
			p.peers[m.Peer] = false
			p.weight += m.GetWeight()
			p.aggregatedWeight += m.GetWeight()
		} else {
			partitionsByValue[m.Value] = &partition{
				value:            m.Value,
				weight:           m.GetWeight(),
				aggregatedWeight: m.GetWeight(),
				peers: map[peer.ID]bool{
					m.Peer: false,
				},
			}
		}
	}

	for _, p := range partitionsByValue {
		partitions = append(partitions, p)
	}
	return partitions
}

// sortedPeers returns a sorted list of peers without selecting from
// the same partition consecutively, when possible.
func (pnedm *partitionedMetric) sortedPeers() []peer.ID {
	peers := []peer.ID{}
	for {
		peer := pnedm.chooseNext()
		if peer == "" {
			break
		}
		peers = append(peers, peer)
	}
	return peers
}

// chooseNext selects the next peer from partitions, avoiding repetition.
func (pnedm *partitionedMetric) chooseNext() peer.ID {
	lenp := len(pnedm.partitions)
	if lenp == 0 || pnedm.noMore {
		return ""
	}

	var peer peer.ID
	curPartition := pnedm.partitions[pnedm.curChoosingIndex]
	done := 0
	for {
		if curPartition.sub != nil {
			peer = curPartition.sub.chooseNext()
		} else {
			for pid, used := range curPartition.peers {
				if !used {
					peer = pid
					curPartition.peers[pid] = true
					break
				}
			}
		}

		pnedm.curChoosingIndex = (pnedm.curChoosingIndex + 1) % lenp
		curPartition = pnedm.partitions[pnedm.curChoosingIndex]
		done++

		if peer != "" {
			break
		}

		if done == lenp {
			pnedm.noMore = true
			break
		}
	}

	return peer
}

// Allocate produces a sorted list of cluster peer IDs based on metrics.
func (a *Allocator) Allocate(
	ctx context.Context,
	c api.Cid,
	current, candidates, priority api.MetricsSet,
) ([]peer.ID, error) {
	candidatePartition := partitionMetrics(candidates, a.config.AllocateBy)
	priorityPartition := partitionMetrics(priority, a.config.AllocateBy)

	logger.Debugf("Balanced allocator partitions:\n%s\n", printPartition(candidatePartition, 0))

	first := priorityPartition.sortedPeers()
	last := candidatePartition.sortedPeers()

	return append(first, last...), nil
}

// Metrics returns the metrics that have been registered with this allocator.
func (a *Allocator) Metrics() []string {
	return a.config.AllocateBy
}

// printPartition returns a string representation of partitions for debugging.
func printPartition(m *partitionedMetric, ind int) string {
	str := ""
	indent := func() {
		for i := 0; i < ind+2; i++ {
			str += " "
		}
	}

	for _, p := range m.partitions {
		indent()
		str += fmt.Sprintf(" | %s:%s - %d - [", m.metricName, p.value, p.weight)
		for p, u := range p.peers {
			str += fmt.Sprintf("%s|%t, ", p, u)
		}
		str += "]\n"
		if p.sub != nil {
			str += printPartition(p.sub, ind+2)
		}
	}
	return str
}
