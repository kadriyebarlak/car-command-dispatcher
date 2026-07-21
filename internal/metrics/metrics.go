package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	CommandsTotal   *prometheus.CounterVec // labeled by outcome
	CarSendDuration prometheus.Histogram
	RetriesTotal    prometheus.Counter // total retry re-publishes
	PendingRetries  prometheus.Gauge   //commands currently awaiting retry
}

func New() *Metrics {
	return &Metrics{
		CommandsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "car_commands_total",
				Help: "Total commands processed, labeled by outcome.",
			},
			[]string{"outcome"}, // e.g. published, acknowledged, failed, dead
		),
		CarSendDuration: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "car_send_duration_seconds",
				Help:    "Time taken for a car send call.",
				Buckets: prometheus.DefBuckets,
			},
		),
		RetriesTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "car_command_retries_total",
				Help: "Total number of commands re-published for retry.",
			},
		),
		PendingRetries: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "car_commands_pending_retries",
				Help: "Number of commands currently awaiting retry.",
			},
		),
	}
}
