package heartbeat

import (
	"fmt"
	"time"
)

// Config cấu hình chu kỳ heartbeat và ngưỡng timeout.
// Ràng buộc: 0 < Interval < SuspectTimeout < DeadTimeout.
type Config struct {
	Interval       time.Duration
	SuspectTimeout time.Duration
	DeadTimeout    time.Duration
}

// Validate kiểm tra Config.
func (c Config) Validate() error {
	if c.Interval <= 0 {
		return fmt.Errorf("%w: interval must be > 0", ErrInvalidConfig)
	}
	if c.SuspectTimeout <= 0 {
		return fmt.Errorf("%w: suspect timeout must be > 0", ErrInvalidConfig)
	}
	if c.DeadTimeout <= 0 {
		return fmt.Errorf("%w: dead timeout must be > 0", ErrInvalidConfig)
	}
	if c.Interval >= c.SuspectTimeout {
		return fmt.Errorf("%w: interval must be < suspect timeout", ErrInvalidConfig)
	}
	if c.SuspectTimeout >= c.DeadTimeout {
		return fmt.Errorf("%w: suspect timeout must be < dead timeout", ErrInvalidConfig)
	}
	return nil
}
