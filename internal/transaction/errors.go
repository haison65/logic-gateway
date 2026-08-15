package transaction

import (
	"errors"
	"time"
)

// DefaultTimeout khớp skeleton configs (5000ms).
const DefaultTimeout = 5 * time.Second

var (
	ErrManagerClosed      = errors.New("transaction manager closed")
	ErrTransactionExists  = errors.New("transaction already exists")
	ErrUnknownTransaction = errors.New("unknown transaction")
	ErrInvalidTransaction = errors.New("invalid transaction")
	ErrInvalidResponse    = errors.New("invalid transaction response")
	ErrTransactionTimeout = errors.New("transaction timeout")
	ErrInvalidRequest     = errors.New("invalid data request")
	ErrInvalidNode        = errors.New("invalid destination node")
)
