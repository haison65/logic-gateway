// Package protocol dựng Envelope UDP theo contract protobuf.
package protocol

import (
	"time"

	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

const EnvelopeVersion = 1

// Reply metadata: đảo source/destination, giữ transaction_id và trace_id.
func Reply(nodeID uint32, in *pb.Envelope, typ pb.MessageType) *pb.Envelope {
	ts := time.Now().UnixMilli()
	if ts < 0 {
		ts = 0
	}
	out := &pb.Envelope{
		Version:      EnvelopeVersion,
		Type:         typ,
		SourceNodeId: nodeID,
		TimestampMs:  uint64(ts),
	}
	if in != nil {
		if in.GetVersion() != 0 {
			out.Version = in.GetVersion()
		}
		out.DestinationNodeId = in.GetSourceNodeId()
		out.TransactionId = in.GetTransactionId()
		out.TraceId = in.GetTraceId()
	}
	return out
}

// ErrorEnvelope tạo MESSAGE_TYPE_ERROR + Envelope.error.
func ErrorEnvelope(nodeID uint32, in *pb.Envelope, code pb.ErrorCode, message string) *pb.Envelope {
	out := Reply(nodeID, in, pb.MessageType_MESSAGE_TYPE_ERROR)
	out.Body = &pb.Envelope_Error{Error: &pb.ErrorMessage{
		Code:    code,
		Message: message,
	}}
	return out
}

// DataResponseEnvelope tạo DATA_RESPONSE, giữ transaction_id.
func DataResponseEnvelope(nodeID uint32, in *pb.Envelope, messageID, status uint32, payload []byte) *pb.Envelope {
	out := Reply(nodeID, in, pb.MessageType_MESSAGE_TYPE_DATA_RESPONSE)
	out.Body = &pb.Envelope_DataResponse{DataResponse: &pb.DataResponse{
		MessageId: messageID,
		Status:    status,
		Payload:   payload,
	}}
	return out
}
