package heartbeat

import "time"

// Clock cung cấp thời điểm hiện tại; production dùng thời gian hệ thống.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func resolveClock(c Clock) Clock {
	if c == nil {
		return realClock{}
	}
	return c
}
