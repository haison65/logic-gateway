package metrics

import (
	"github.com/haison65/logic-gateway/internal/transaction"
)

// Compile-time: Metrics implements transaction.MetricsSink.
var _ transaction.MetricsSink = (*Metrics)(nil)
