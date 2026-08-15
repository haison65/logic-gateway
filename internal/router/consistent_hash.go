package router

import (
	"hash/fnv"
	"sort"
	"strconv"

	"github.com/haison65/logic-gateway/internal/registry"
)

// DefaultVirtualNodes là số điểm ảo trên vòng hash cho mỗi node vật lý.
const DefaultVirtualNodes = 64

type vnode struct {
	hash uint64
	node *registry.Node
}

// ConsistentHash ánh xạ key ổn định lên candidate hiện tại (FNV-1a 64-bit).
// Vòng hash được dựng lại mỗi lần Select để luôn theo tập node mới.
type ConsistentHash struct {
	virtualNodes int
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

	ring := c.buildRing(nodes)
	h := hashString(key)
	i := sort.Search(len(ring), func(i int) bool { return ring[i].hash >= h })
	if i == len(ring) {
		i = 0
	}
	return ring[i].node, nil
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
