package router

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"

	"github.com/haison65/logic-gateway/internal/registry"
)

// DefaultVirtualNodes là số điểm ảo trên vòng hash cho mỗi node vật lý.
const DefaultVirtualNodes = 64

type vnode struct {
	hash uint64
	node *registry.Node
}

// ConsistentHash ánh xạ key ổn định lên candidate hiện tại (FNV-1a 64-bit).
// Ring được cache theo fingerprint tập node ID — rebuild khi tập candidate đổi (P0.1).
type ConsistentHash struct {
	virtualNodes int

	mu       sync.Mutex
	cachedFP uint64
	ring     []vnode
}

// NewConsistentHash tạo strategy. virtualNodes <= 0 thì dùng DefaultVirtualNodes.
func NewConsistentHash(virtualNodes int) *ConsistentHash {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}
	return &ConsistentHash{virtualNodes: virtualNodes}
}

// Select chọn node theo vị trí hash(key) trên vòng. Cùng key + cùng tập node → cùng kết quả.
func (c *ConsistentHash) Select(nodes []*registry.Node, key string) (*registry.Node, error) {
	if c == nil {
		return nil, ErrEmptyCandidates
	}
	if len(nodes) == 0 {
		return nil, ErrEmptyCandidates
	}
	if key == "" {
		return nil, ErrInvalidRoutingKey
	}

	fp := nodesFingerprint(nodes)
	c.mu.Lock()
	if c.ring == nil || fp != c.cachedFP {
		c.ring = c.buildRing(nodes)
		c.cachedFP = fp
	}
	ring := c.ring
	c.mu.Unlock()

	h := hashString(key)
	i := sort.Search(len(ring), func(i int) bool { return ring[i].hash >= h })
	if i == len(ring) {
		i = 0
	}
	return ring[i].node, nil
}

func nodesFingerprint(nodes []*registry.Node) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	for _, n := range nodes {
		if n == nil {
			continue
		}
		id := uint64(n.ID)
		buf[0] = byte(id)
		buf[1] = byte(id >> 8)
		buf[2] = byte(id >> 16)
		buf[3] = byte(id >> 24)
		buf[4] = byte(id >> 32)
		buf[5] = byte(id >> 40)
		buf[6] = byte(id >> 48)
		buf[7] = byte(id >> 56)
		_, _ = h.Write(buf[:])
	}
	return h.Sum64()
}

func (c *ConsistentHash) buildRing(nodes []*registry.Node) []vnode {
	ring := make([]vnode, 0, len(nodes)*c.virtualNodes)
	for _, n := range nodes {
		id := strconv.FormatUint(uint64(n.ID), 10)
		for v := 0; v < c.virtualNodes; v++ {
			ring = append(ring, vnode{
				hash: hashString(id + "#" + strconv.Itoa(v)),
				node: n,
			})
		}
	}
	sort.Slice(ring, func(i, j int) bool {
		if ring[i].hash == ring[j].hash {
			return ring[i].node.ID < ring[j].node.ID
		}
		return ring[i].hash < ring[j].hash
	})
	return ring
}

func hashString(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
