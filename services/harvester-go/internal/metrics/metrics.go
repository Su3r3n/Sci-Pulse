package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds all Prometheus metrics exposed by harvester-go.
type Metrics struct {
	FetchedTotal               *prometheus.CounterVec
	ValidTotal                 *prometheus.CounterVec
	DLQTotal                   *prometheus.CounterVec
	HTTPRequestsTotal          *prometheus.CounterVec
	BudgetRemaining            *prometheus.GaugeVec
	CacheHitsTotal             *prometheus.CounterVec
	ProduceLatencySeconds      *prometheus.HistogramVec
	LastSuccessTimestampSecond *prometheus.GaugeVec
	RegistryProjects           *prometheus.GaugeVec
}

// New creates and registers all harvester metrics with reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		FetchedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "harvester_fetched_total",
			Help: "Total number of raw records fetched from sources.",
		}, []string{"source"}),
		ValidTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "harvester_valid_total",
			Help: "Total number of valid records mapped and validated.",
		}, []string{"source"}),
		DLQTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "harvester_dlq_total",
			Help: "Total number of records routed to DLQ.",
		}, []string{"source", "reason"}),
		HTTPRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "harvester_http_requests_total",
			Help: "Total HTTP requests made by harvester sources.",
		}, []string{"source", "code"}),
		BudgetRemaining: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "harvester_budget_remaining",
			Help: "Remaining daily API request budget per source.",
		}, []string{"source"}),
		CacheHitsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "harvester_cache_hits_total",
			Help: "Total number of local cache hits.",
		}, []string{"source"}),
		ProduceLatencySeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "harvester_produce_latency_seconds",
			Help:    "Kafka produce acknowledgment latency in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"topic"}),
		LastSuccessTimestampSecond: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "harvester_last_success_timestamp_seconds",
			Help: "Unix timestamp of last successful harvest batch per source and scope.",
		}, []string{"source", "scope"}),
		RegistryProjects: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "harvester_registry_projects",
			Help: "Number of tracked projects known to harvester by status.",
		}, []string{"status"}),
	}

	if reg != nil {
		reg.MustRegister(
			m.FetchedTotal,
			m.ValidTotal,
			m.DLQTotal,
			m.HTTPRequestsTotal,
			m.BudgetRemaining,
			m.CacheHitsTotal,
			m.ProduceLatencySeconds,
			m.LastSuccessTimestampSecond,
			m.RegistryProjects,
		)
	}
	return m
}

func (m *Metrics) IncFetched(source string) {
	if m != nil {
		m.FetchedTotal.WithLabelValues(source).Inc()
	}
}

func (m *Metrics) IncValid(source string) {
	if m != nil {
		m.ValidTotal.WithLabelValues(source).Inc()
	}
}

func (m *Metrics) IncDLQ(source, reason string) {
	if m != nil {
		m.DLQTotal.WithLabelValues(source, reason).Inc()
	}
}

func (m *Metrics) IncHTTPRequest(source string, statusCode int) {
	if m != nil {
		m.HTTPRequestsTotal.WithLabelValues(source, strconv.Itoa(statusCode)).Inc()
	}
}

func (m *Metrics) SetBudgetRemaining(source string, remaining int) {
	if m != nil {
		m.BudgetRemaining.WithLabelValues(source).Set(float64(remaining))
	}
}

func (m *Metrics) IncCacheHit(source string) {
	if m != nil {
		m.CacheHitsTotal.WithLabelValues(source).Inc()
	}
}

func (m *Metrics) ObserveProduceLatency(topic string, dur time.Duration) {
	if m != nil {
		m.ProduceLatencySeconds.WithLabelValues(topic).Observe(dur.Seconds())
	}
}

func (m *Metrics) SetLastSuccess(source, scope string, ts time.Time) {
	if m != nil {
		m.LastSuccessTimestampSecond.WithLabelValues(source, scope).Set(float64(ts.Unix()))
	}
}

func (m *Metrics) SetRegistryProjects(status string, count int) {
	if m != nil {
		m.RegistryProjects.WithLabelValues(status).Set(float64(count))
	}
}
