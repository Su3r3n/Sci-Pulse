package repository

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// PgxPoolCollector exports pgxpool statistics as Prometheus metrics.
type PgxPoolCollector struct {
	pool *pgxpool.Pool

	acquiredConns        *prometheus.Desc
	idleConns            *prometheus.Desc
	totalConns           *prometheus.Desc
	maxConns             *prometheus.Desc
	constructingConns    *prometheus.Desc
	acquireCount         *prometheus.Desc
	emptyAcquireCount    *prometheus.Desc
	canceledAcquireCount *prometheus.Desc
	acquireDuration      *prometheus.Desc
}

// NewPgxPoolCollector creates a Prometheus collector for the given pgxpool.Pool.
func NewPgxPoolCollector(pool *pgxpool.Pool, dbName string) *PgxPoolCollector {
	labels := prometheus.Labels{"db": dbName}
	return &PgxPoolCollector{
		pool: pool,
		acquiredConns: prometheus.NewDesc(
			"pgxpool_acquired_conns",
			"Number of currently acquired connections in the pool.",
			nil, labels,
		),
		idleConns: prometheus.NewDesc(
			"pgxpool_idle_conns",
			"Number of currently idle connections in the pool.",
			nil, labels,
		),
		totalConns: prometheus.NewDesc(
			"pgxpool_total_conns",
			"Total number of connections currently in the pool.",
			nil, labels,
		),
		maxConns: prometheus.NewDesc(
			"pgxpool_max_conns",
			"Maximum configured connections in the pool.",
			nil, labels,
		),
		constructingConns: prometheus.NewDesc(
			"pgxpool_constructing_conns",
			"Number of connections currently being constructed.",
			nil, labels,
		),
		acquireCount: prometheus.NewDesc(
			"pgxpool_acquire_count_total",
			"Cumulative count of successful acquires from the pool.",
			nil, labels,
		),
		emptyAcquireCount: prometheus.NewDesc(
			"pgxpool_empty_acquire_count_total",
			"Cumulative count of acquires that waited for a connection because the pool was empty.",
			nil, labels,
		),
		canceledAcquireCount: prometheus.NewDesc(
			"pgxpool_canceled_acquire_count_total",
			"Cumulative count of acquires canceled by context.",
			nil, labels,
		),
		acquireDuration: prometheus.NewDesc(
			"pgxpool_acquire_duration_seconds_total",
			"Total time spent acquiring connections from the pool in seconds.",
			nil, labels,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *PgxPoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.acquiredConns
	ch <- c.idleConns
	ch <- c.totalConns
	ch <- c.maxConns
	ch <- c.constructingConns
	ch <- c.acquireCount
	ch <- c.emptyAcquireCount
	ch <- c.canceledAcquireCount
	ch <- c.acquireDuration
}

// Collect implements prometheus.Collector.
func (c *PgxPoolCollector) Collect(ch chan<- prometheus.Metric) {
	if c.pool == nil {
		return
	}
	st := c.pool.Stat()
	ch <- prometheus.MustNewConstMetric(c.acquiredConns, prometheus.GaugeValue, float64(st.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(c.idleConns, prometheus.GaugeValue, float64(st.IdleConns()))
	ch <- prometheus.MustNewConstMetric(c.totalConns, prometheus.GaugeValue, float64(st.TotalConns()))
	ch <- prometheus.MustNewConstMetric(c.maxConns, prometheus.GaugeValue, float64(st.MaxConns()))
	ch <- prometheus.MustNewConstMetric(c.constructingConns, prometheus.GaugeValue, float64(st.ConstructingConns()))
	ch <- prometheus.MustNewConstMetric(c.acquireCount, prometheus.CounterValue, float64(st.AcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.emptyAcquireCount, prometheus.CounterValue, float64(st.EmptyAcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.canceledAcquireCount, prometheus.CounterValue, float64(st.CanceledAcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.acquireDuration, prometheus.CounterValue, st.AcquireDuration().Seconds())
}
