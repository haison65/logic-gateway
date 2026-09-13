package transaction

import "time"

// NopMetrics là MetricsSink no-op cho test stub.
type NopMetrics struct{}

func (NopMetrics) AddLogicRouted(uint32, string)          {}
func (NopMetrics) AddGWUDPDataRequestTX(int64)            {}
func (NopMetrics) AddGWUDPSendError(int64)                {}
func (NopMetrics) ObserveTransactionTotal(time.Duration)  {}
func (NopMetrics) ObserveManagerWait(time.Duration)       {}
func (NopMetrics) AddManagerCreated(int64)                {}
func (NopMetrics) AddManagerCompleted(int64)              {}
func (NopMetrics) AddManagerTimeout(int64)                {}
func (NopMetrics) AddManagerCompleteNotFound(int64)       {}
func (NopMetrics) SetManagerPending(int64)                {}
func (NopMetrics) AddFailoverRetry(int64)                 {}
