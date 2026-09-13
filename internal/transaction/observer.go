package transaction

import "time"

// MetricsSink là hook instrumentation tùy chọn (nil = tắt).
// *metrics.Metrics triển khai đầy đủ; test stub chỉ cần method được gọi.
type MetricsSink interface {
	AddLogicRouted(nodeID uint32, nodeName string)
	AddGWUDPDataRequestTX(n int64)
	AddGWUDPSendError(n int64)
	ObserveTransactionTotal(d time.Duration)
	ObserveManagerWait(d time.Duration)
	AddManagerCreated(n int64)
	AddManagerCompleted(n int64)
	AddManagerTimeout(n int64)
	AddManagerCompleteNotFound(n int64)
	SetManagerPending(n int64)
	AddFailoverRetry(n int64)
}
