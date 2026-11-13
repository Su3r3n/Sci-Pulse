// Package metrics defines the loadgen Prometheus metrics (§5.6).
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics holds the load generator gauges and counters.
type Metrics struct {
	OfferedRate   prometheus.Gauge
	AchievedRate  prometheus.Gauge
	ProducerError prometheus.Counter
}

// New registers the loadgen metrics on reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		OfferedRate: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "loadgen_offered_rate",
			Help: "Events per second the current stress test is configured to offer.",
		}),
		AchievedRate: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "loadgen_achieved_rate",
			Help: "Events per second actually acknowledged by Kafka.",
		}),
		ProducerError: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "loadgen_producer_errors_total",
			Help: "Producer errors while sending synthetic events.",
		}),
	}
	reg.MustRegister(m.OfferedRate, m.AchievedRate, m.ProducerError)
	return m
}
