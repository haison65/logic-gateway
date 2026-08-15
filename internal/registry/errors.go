package registry

import "errors"

var (
	// ErrNotFound được trả về khi node không tồn tại trong registry.
	ErrNotFound = errors.New("node not found")

	// ErrInvalidNode được trả về khi Node không đủ điều kiện lưu trữ.
	ErrInvalidNode = errors.New("invalid node")
)
