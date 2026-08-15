package protocol

import (
	"testing"

	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

func TestErrorEnvelope(t *testing.T) {
	t.Parallel()
	in := &pb.Envelope{TransactionId: 5, TraceId: "t", SourceNodeId: 2}
	out := ErrorEnvelope(1, in, pb.ErrorCode_ERROR_CODE_NO_ROUTING_TARGET, "none")
	if out.GetType() != pb.MessageType_MESSAGE_TYPE_ERROR {
		t.Fatal(out.GetType())
	}
	if out.GetTransactionId() != 5 || out.GetError().GetCode() != pb.ErrorCode_ERROR_CODE_NO_ROUTING_TARGET {
		t.Fatalf("%+v", out)
	}
}
