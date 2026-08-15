package logicgatewayv1

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestEnvelopeMarshalUnmarshal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		msgType    MessageType
		body       isEnvelope_Body
		assertBody func(t *testing.T, decoded *Envelope)
	}{
		{
			name:    "register_request",
			msgType: MessageType_MESSAGE_TYPE_REGISTER_REQUEST,
			body: &Envelope_RegisterRequest{
				RegisterRequest: &RegisterRequest{
					NodeId:     42,
					NodeType:   NodeType_NODE_TYPE_LOGIC,
					InstanceId: "logic-1",
					Ip:         "10.0.0.12",
					Port:       9100,
					Services: []*ServiceCapability{
						{
							ServiceId:    100,
							ServiceName:  "call",
							MessageTypes: []uint32{1001, 1002, 1003},
						},
					},
				},
			},
			assertBody: func(t *testing.T, decoded *Envelope) {
				t.Helper()
				got, ok := decoded.GetBody().(*Envelope_RegisterRequest)
				if !ok {
					t.Fatalf("body type = %T, want *Envelope_RegisterRequest", decoded.GetBody())
				}
				req := got.RegisterRequest
				if req.GetNodeId() != 42 || req.GetPort() != 9100 {
					t.Fatalf("register request = %+v", req)
				}
			},
		},
		{
			name:    "heartbeat_request",
			msgType: MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST,
			body: &Envelope_HeartbeatRequest{
				HeartbeatRequest: &HeartbeatRequest{
					NodeId:            42,
					Sequence:          17,
					TimestampMs:       1_725_000_000_100,
					Load:              35,
					ActiveTransaction: 4,
				},
			},
			assertBody: func(t *testing.T, decoded *Envelope) {
				t.Helper()
				got, ok := decoded.GetBody().(*Envelope_HeartbeatRequest)
				if !ok {
					t.Fatalf("body type = %T, want *Envelope_HeartbeatRequest", decoded.GetBody())
				}
				if got.HeartbeatRequest.GetSequence() != 17 {
					t.Fatalf("heartbeat sequence = %d, want 17", got.HeartbeatRequest.GetSequence())
				}
			},
		},
		{
			name:    "data_request",
			msgType: MessageType_MESSAGE_TYPE_DATA_REQUEST,
			body: &Envelope_DataRequest{
				DataRequest: &DataRequest{
					MessageId: 1001,
					SessionId: "sess-9",
					Payload:   []byte{0x01, 0x02, 0x03},
				},
			},
			assertBody: func(t *testing.T, decoded *Envelope) {
				t.Helper()
				got, ok := decoded.GetBody().(*Envelope_DataRequest)
				if !ok {
					t.Fatalf("body type = %T, want *Envelope_DataRequest", decoded.GetBody())
				}
				if got.DataRequest.GetMessageId() != 1001 {
					t.Fatalf("data message_id = %d, want 1001", got.DataRequest.GetMessageId())
				}
			},
		},
		{
			name:    "error_message",
			msgType: MessageType_MESSAGE_TYPE_ERROR,
			body: &Envelope_Error{
				Error: &ErrorMessage{
					Code:    ErrorCode_ERROR_CODE_NO_ROUTING_TARGET,
					Message: "no active logic node for message_id 1001",
				},
			},
			assertBody: func(t *testing.T, decoded *Envelope) {
				t.Helper()
				got, ok := decoded.GetBody().(*Envelope_Error)
				if !ok {
					t.Fatalf("body type = %T, want *Envelope_Error", decoded.GetBody())
				}
				if got.Error.GetCode() != ErrorCode_ERROR_CODE_NO_ROUTING_TARGET {
					t.Fatalf("error code = %v", got.Error.GetCode())
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := &Envelope{
				Version:           1,
				Type:              tt.msgType,
				SourceNodeId:      100,
				DestinationNodeId: 200,
				TransactionId:     9_001,
				TimestampMs:       1_725_000_000_000,
				TraceId:           "trace-abc",
				Body:              tt.body,
			}

			wire, err := proto.Marshal(original)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if len(wire) == 0 {
				t.Fatal("marshal produced empty buffer")
			}

			decoded := &Envelope{}
			if err := proto.Unmarshal(wire, decoded); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}

			if decoded.GetVersion() != 1 {
				t.Errorf("version = %d, want 1", decoded.GetVersion())
			}
			if decoded.GetType() != tt.msgType {
				t.Errorf("type = %v, want %v", decoded.GetType(), tt.msgType)
			}
			if decoded.GetSourceNodeId() != 100 {
				t.Errorf("source_node_id = %d, want 100", decoded.GetSourceNodeId())
			}
			if decoded.GetDestinationNodeId() != 200 {
				t.Errorf("destination_node_id = %d, want 200", decoded.GetDestinationNodeId())
			}
			if decoded.GetTransactionId() != 9_001 {
				t.Errorf("transaction_id = %d, want 9001", decoded.GetTransactionId())
			}
			if decoded.GetTimestampMs() != 1_725_000_000_000 {
				t.Errorf("timestamp_ms = %d, want 1725000000000", decoded.GetTimestampMs())
			}
			if decoded.GetTraceId() != "trace-abc" {
				t.Errorf("trace_id = %q, want %q", decoded.GetTraceId(), "trace-abc")
			}

			tt.assertBody(t, decoded)

			if !proto.Equal(original, decoded) {
				t.Fatalf("decoded message not equal to original\noriginal = %v\ndecoded  = %v", original, decoded)
			}
		})
	}
}

func TestEnvelopeUnmarshalInvalid(t *testing.T) {
	t.Parallel()

	decoded := &Envelope{}
	if err := proto.Unmarshal([]byte{0x80}, decoded); err == nil {
		t.Fatal("expected unmarshal error for truncated varint")
	}
}
