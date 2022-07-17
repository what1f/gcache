package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type Hash func([]byte) uint32

type Map struct {
	hash     Hash
	replicas int // 虚拟节点个数
	keys     []int
	hashMap  map[int]string
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		hash:     fn,
		replicas: replicas,
		hashMap:  map[int]string{},
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}
	hash := int(m.hash([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	return m.hashMap[m.keys[idx%len(m.keys)]] // when hash > m.keys[last] => idx > len(m.keys). back to first replica
}

func (m *Map) Remove(keys ...string) {
	keyMap := make(map[string]struct{})
	for _, k := range keys {
		keyMap[k] = struct{}{}
	}
	newHashMap := make(map[int]string)
	var newKeys []int
	for k, v := range m.hashMap {
		if _, ok := keyMap[v]; !ok {
			newHashMap[k] = v
			newKeys = append(newKeys, k)
		}
	}
	sort.Ints(newKeys)
	m.keys = newKeys
	m.hashMap = newHashMap
}
