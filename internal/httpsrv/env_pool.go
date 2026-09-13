package httpsrv

import (
	"sync"

	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

// envSlot pools Envelope + DataRequest oneof graph (Iter #21).
// Internal pointers stay fixed across reuse.
type envSlot struct {
	env pb.Envelope
	wr  pb.Envelope_DataRequest
	dr  pb.DataRequest
}

func newEnvSlot() *envSlot {
	s := &envSlot{}
	s.wr.DataRequest = &s.dr
	s.env.Body = &s.wr
	return s
}

var envPool = sync.Pool{
	New: func() any { return newEnvSlot() },
}

func acquireEnvSlot() *envSlot {
	return envPool.Get().(*envSlot)
}

func releaseEnvSlot(s *envSlot) {
	if s == nil {
		return
	}
	s.dr.Payload = nil
	s.env.Reset()
	s.dr = pb.DataRequest{}
	s.wr = pb.Envelope_DataRequest{DataRequest: &s.dr}
	s.env.Body = &s.wr
	envPool.Put(s)
}
