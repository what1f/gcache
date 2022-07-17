package gcache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/what1f/gcache/consistenthash"
	pb "github.com/what1f/gcache/gcachepb"
)

const (
	_defaultBasePath = "/_cache/"
	_defaultReplicas = 50
)

type HTTPPool struct {
	// 当前节点的基础路径，例如 localhost:8000
	self     string
	basePath string
	mu       sync.Mutex
	peers    *consistenthash.Map
	getters  map[string]*httpGetter
	opts     HTTPPoolOptions
}

type HTTPPoolOptions struct {
	BasePath string
	Replicas int
	HashFn   consistenthash.Hash
}

type httpGetter struct {
	baseURL   string
	transport func(context.Context) http.RoundTripper
}

var bufferPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

func NewHTTPPool(self string) *HTTPPool {
	p := NewHTTPPoolOpts(self, nil)
	http.Handle(self, p)
	return p
}

func NewHTTPPoolOpts(self string, o *HTTPPoolOptions) *HTTPPool {
	p := &HTTPPool{
		self:    self,
		getters: make(map[string]*httpGetter),
	}
	if o != nil {
		p.opts = *o
	}
	if p.opts.BasePath == "" {
		p.opts.BasePath = _defaultBasePath
	}
	if p.opts.Replicas == 0 {
		p.opts.Replicas = _defaultReplicas
	}
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)
	RegisterPeerPicker(func() PeerPicker { return p })
	return p
}

func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)
	p.peers.Add(peers...)
	p.getters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.getters[peer] = &httpGetter{baseURL: peer + p.opts.BasePath}
	}
}

func (p *HTTPPool) Add(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.peers == nil {
		p.Set(peers...)
		return
	}
	p.peers.Add(peers...)
	for _, peer := range peers {
		p.getters[peer] = &httpGetter{baseURL: peer + p.opts.BasePath}
	}
}

func (p *HTTPPool) Remove(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers.Remove(peers...)
	for _, peer := range peers {
		delete(p.getters, peer)
	}
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Parse request.
	if !strings.HasPrefix(r.URL.Path, p.opts.BasePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	parts := strings.SplitN(r.URL.Path[len(p.opts.BasePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	groupName := parts[0]
	key := parts[1]

	// Fetch the value for this group/key.
	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}

	value, err := group.Get(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write the value to the response body as a proto message.
	body, err := proto.Marshal(&pb.Response{Value: value.ByteSlice()})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Write(body)
}

func (h *httpGetter) Get(ctx context.Context, in *pb.Request, out *pb.Response) error {
	u := fmt.Sprintf("%v%v/%v",
		h.baseURL,
		url.QueryEscape(in.GetGroup()),
		url.QueryEscape(in.GetKey()),
	)
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}
	req.WithContext(ctx)
	tr := http.DefaultTransport
	if h.transport != nil {
		tr = h.transport(ctx)
	}
	res, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned:%v", res.Status)
	}
	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()
	defer bufferPool.Put(b)
	_, err = io.Copy(b, res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}
	if err = proto.Unmarshal(b.Bytes(), out); err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	return nil
}

func (p *HTTPPool) PickPeer(key string) ProtoGetter {
	if peer := p.peers.Get(key); peer != p.self {
		return p.getters[peer]
	}
	return nil
}
