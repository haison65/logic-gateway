// Package registration xử lý nghiệp vụ đăng ký node phía HTTP2GW.
//
// Gói này dịch protobuf RegisterRequest thành Node domain rồi gọi Registry.
// Không đụng socket UDP.
package registration

import "errors"

var (
	// ErrInvalidRequest được trả về khi RegisterRequest không hợp lệ.
	ErrInvalidRequest = errors.New("invalid register request")
)
