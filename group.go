package gcache

import (
	"context"
	"math/rand"
	"sync"

	pb "github.com/what1f/gcache/gcachepb"
	"github.com/what1f/gcache/singleflight"
)

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)

	initPeerServerOnce sync.Once
	initPeerServer     func()
)

type Group struct {
	name       string
	getter     Getter
	peersOnce  sync.Once
	peers      PeerPicker
	cacheBytes int64 // sizeof(mainCache + hotCache)

	mainCache cache

	hotCache cache

	loadGroup *singleflight.Group

	_ int32 // TODO 内存对其相关
}

func (g *Group) Name() string {
	return g.name
}

func GetGroup(name string) *Group {
	mu.RLock()
	defer mu.RUnlock()
	return groups[name]
}

func (g *Group) initPeers() {
	if g.peers == nil {
		if p := portPicker(g.Name()); p != nil {
			g.peers = p
		}
	}
}

// newGroupHook, 创建group的回调函数
var newGroupHook func(*Group)

func callInitPeerServer() {
	if initPeerServer != nil {
		initPeerServer()
	}
}

// NewGroup cacheBytes 当前group最大缓存字节数，getter 缓存未命中时用于回源读数据的回调函数
func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("getter is nil")
	}
	mu.Lock()
	defer mu.Unlock()
	initPeerServerOnce.Do(callInitPeerServer)
	if _, dup := groups[name]; dup {
		panic("duplicate registration of group" + name)
	}
	g := &Group{
		name:       name,
		getter:     getter,
		cacheBytes: cacheBytes,
		loadGroup:  &singleflight.Group{},
	}

	if newGroupHook != nil {
		newGroupHook(g)
	}
	groups[name] = g
	return g
}

// 从本地cache中查找
func (g *Group) lookupCache(key string) (value ByteView, ok bool) {
	if g.cacheBytes <= 0 {
		return
	}
	value, ok = g.mainCache.get(key)
	if ok {
		return
	}
	value, ok = g.hotCache.get(key)
	return
}

func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
	g.peersOnce.Do(g.initPeers)
	// 在本地缓存中查找
	if v, hit := g.lookupCache(key); hit {
		return v, nil
	}

	return g.load(ctx, key)
}

func (g *Group) load(ctx context.Context, key string) (value ByteView, err error) {
	view, err := g.loadGroup.Do(key, func() (interface{}, error) {
		if v, hit := g.lookupCache(key); hit {
			return v, nil
		}
		var val ByteView
		var err error
		if peer := g.peers.PickPeer(key); peer != nil {
			return g.getFromPeer(ctx, peer, key)
		}
		val, err = g.getLocally(ctx, key)
		if err != nil {
			return ByteView{}, nil
		}
		g.populate(key, val, &g.mainCache)
		return val, nil
	})
	if err == nil {
		value = view.(ByteView)
	}
	return
}

func (g *Group) getFromPeer(ctx context.Context, peer ProtoGetter, key string) (ByteView, error) {
	req := &pb.Request{
		Group: g.Name(),
		Key:   key,
	}
	resp := &pb.Response{}
	err := peer.Get(ctx, req, resp)
	if err != nil {
		return ByteView{}, err
	}
	val := ByteView{b: resp.Value}

	// 0.1的概率将远程节点的数据缓存到本地的hotCache中（热点数据）
	if rand.Intn(10) == 0 {
		g.populate(key, val, &g.hotCache)
	}
	return val, nil
}

func (g *Group) getLocally(ctx context.Context, key string) (ByteView, error) {
	val, err := g.getter.Get(ctx, key)
	if err != nil {
		return ByteView{}, err
	}
	return val, nil
}

func (g *Group) populate(key string, val ByteView, cache *cache) {
	if g.cacheBytes <= 0 {
		return
	}
	cache.add(key, val)
	for {
		mainBytes, hotBytes := g.mainCache.bytes(), g.hotCache.bytes()
		if mainBytes+hotBytes <= g.cacheBytes {
			return
		}
		c := &g.mainCache
		if g.hotCache.bytes() >= g.mainCache.bytes()/8 {
			c = &g.hotCache
		}
		c.removeOldest()
	}
}
