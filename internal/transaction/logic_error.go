package transaction

import (
	"fmt"

	pb "github.com/haison65/logic-gateway/proto/gen/go"
)

// LogicError là lỗi ứng dụng từ Envelope MESSAGE_TYPE_ERROR (design §15).
// Wait trả về error này thay vì timeout → HTTP map đúng, tránh 504 giả.
type LogicError struct {
	Code    pb.ErrorCode
	Message string
}

func (e *LogicError) Error() string {
	if e == nil {
		return "logic error"
	}
	if e.Message != "" {
		return fmt.Sprintf("logic error: %s (%s)", e.Code.String(), e.Message)
	}
	return fmt.Sprintf("logic error: %s", e.Code.String())
}

// LogicErrorFromProto dựng LogicError từ ErrorMessage protobuf (nil-safe).
func LogicErrorFromProto(msg *pb.ErrorMessage) *LogicError {
	if msg == nil {
		return &LogicError{Code: pb.ErrorCode_ERROR_CODE_UNKNOWN}
	}
	return &LogicError{Code: msg.GetCode(), Message: msg.GetMessage()}
}
