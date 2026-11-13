package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsAreRegisteredWithSpecNames(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	m.OfferedRate.Set(25000)
	m.AchievedRate.Set(24000)
	m.ProducerError.Inc()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	want := map[string]bool{
		"loadgen_offered_rate":          false,
		"loadgen_achieved_rate":         false,
		"loadgen_producer_errors_total": false,
	}
	for _, f := range families {
		if _, ok := want[f.GetName()]; ok {
			want[f.GetName()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("metric %s not registered", name)
		}
	}
}
