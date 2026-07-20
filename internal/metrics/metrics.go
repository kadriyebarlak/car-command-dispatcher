package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	CommandsTotal   *prometheus.CounterVec // labeled by outcome
	CarSendDuration prometheus.Histogram
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
	}
}
