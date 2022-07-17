package lru

import (
	"container/list"
)

// Key 可以为能被比较的任意值
type Key interface{}

type Value interface{}

type entry struct {
	key Key
	val Value
}

// Cache 是一个基于LRU的缓存，非并发安全
type Cache struct {
	// 最大缓存数量，0代表不限制
	MaxEntries int
	// 缓存被淘汰后触发的回调函数
	onEvicted func(key Key, value Value)
	ll        *list.List
	cache     map[Key]*list.Element
}

func New() *Cache {
	return &Cache{
		ll:    list.New(),
		cache: make(map[Key]*list.Element),
	}
}

func (c *Cache) SetMaxEntries(n int) {
	c.MaxEntries = n
}

// SetOnEvicted 设置某个缓存被淘汰时的回调函数
func (c *Cache) SetOnEvicted(fn func(Key, Value)) {
	c.onEvicted = fn
}

func (c *Cache) Add(key Key, value Value) {
	if c.cache == nil {
		c.ll = list.New()
		c.cache = make(map[Key]*list.Element)
	}
	// 如果key已存在，将对应的节点移动到表头，并更新对应的值
	if v, hit := c.cache[key]; hit {
		c.ll.MoveToFront(v)
		v.Value.(*entry).val = value
		return
	}
	ele := c.ll.PushFront(&entry{
		key: key,
		val: value,
	})
	c.cache[key] = ele
	if c.MaxEntries != 0 && c.ll.Len() > c.MaxEntries {
		c.RemoveOldest()
	}
}

func (c *Cache) Get(key Key) (val Value, hit bool) {
	if c.cache == nil {
		return nil, false
	}
	val, hit = c.get(key)
	return
}

func (c *Cache) get(key Key) (val Value, hit bool) {
	if c.cache == nil {
		return nil, false
	}
	if ele, hit := c.cache[key]; hit {
		return ele.Value.(*entry).val, true
	}
	return nil, false
}

func (c *Cache) removeElement(e *list.Element) {
	if e == nil {
		return
	}
	c.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(c.cache, kv.key)
	if c.onEvicted != nil {
		c.onEvicted(kv.key, kv.val)
	}
}

// Remove 删除指定key的项
func (c *Cache) Remove(key Key) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele)
	}
}

// RemoveOldest 淘汰最久未使用的项
func (c *Cache) RemoveOldest() {
	if c.cache == nil {
		return
	}
	c.removeElement(c.ll.Back())
}

func (c *Cache) Len() int {
	if c.cache == nil {
		return 0
	}
	return c.ll.Len()
}

// Clear 清空缓存
func (c *Cache) Clear() {
	if c.onEvicted != nil {
		for _, e := range c.cache {
			kv := e.Value.(*entry)
			c.onEvicted(kv.key, kv.val)
		}
	}
	c.ll = nil
	c.cache = nil
}
