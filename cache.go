package gcache

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/what1f/gcache/lru"
)

type Getter interface {
	Get(ctx context.Context, key string) (ByteView, error)
}

type GetterFunc func(ctx context.Context, key string) (ByteView, error)

func (f GetterFunc) Get(ctx context.Context, key string) (ByteView, error) {
	return f(ctx, key)
}

type cache struct {
	mu         sync.RWMutex
	nBytes     int64 // key和value共占的字节数
	lru        *lru.Cache
	nHit, nGet int64 // 命中次数，获取次数
	nEvict     int64 // 淘汰的次数
}

func (c *cache) add(key string, value ByteView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		c.lru = lru.New()
		c.lru.SetOnEvicted(func(key lru.Key, value lru.Value) {
			val := value.(ByteView)
			c.nBytes -= int64(val.Len() + len(key.(string)))
			c.nEvict++
		})
	}
	c.lru.Add(key, value)
	c.nBytes += int64(value.Len() + len(key))
}

func (c *cache) get(key string) (value ByteView, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nGet++
	if c.lru == nil {
		return
	}
	if val, ok := c.lru.Get(key); ok {
		c.nHit++
		return val.(ByteView), true
	}
	return
}

func (c *cache) bytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nBytes
}

func (c *cache) items() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.itemsLocked()
}

func (c *cache) itemsLocked() int64 {
	if c.lru == nil {
		return 0
	}
	return int64(c.lru.Len())
}

type AtomicInt int64

func (i *AtomicInt) Add(n int64) {
	atomic.AddInt64((*int64)(i), n)
}

func (i *AtomicInt) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

func (i *AtomicInt) String() string {
	return strconv.FormatInt(i.Get(), 10)
}

type CacheStats struct {
	Bytes     int64
	Items     int64
	Gets      int64
	Hits      int64
	Evictions int64
}

func (c *cache) stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return CacheStats{
		Bytes:     c.nBytes,
		Items:     c.itemsLocked(),
		Gets:      c.nGet,
		Hits:      c.nHit,
		Evictions: c.nEvict,
	}
}

func (c *cache) removeOldest() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru != nil {
		c.lru.RemoveOldest()
	}
}
