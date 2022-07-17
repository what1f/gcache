package gcache

import (
	"context"

	pb "github.com/what1f/gcache/gcachepb"
)

type ProtoGetter interface {
	Get(ctx context.Context, in *pb.Request, out *pb.Response) error
}

type PeerPicker interface {
	PickPeer(key string) ProtoGetter
}

var (
	portPicker func(groupName string) PeerPicker
)

func RegisterPeerPicker(fn func() PeerPicker) {
	if portPicker != nil {
		panic("RegisterPeerPicker called more than once")
	}
	portPicker = func(_ string) PeerPicker { return fn() }
}
