package query

import (
	"testing"

	"aetherdb/internal/model"
)

func TestParser_SimpleMetric(t *testing.T) {
	p := NewParser()

	q, err := p.Parse("cpu_usage")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if q.MetricName != "cpu_usage" {
		t.Errorf("Expected metric name 'cpu_usage', got '%s'", q.MetricName)
	}

	if len(q.Matchers) != 0 {
		t.Errorf("Expected 0 matchers, got %d", len(q.Matchers))
	}

	if q.Function != "" {
		t.Errorf("Expected no function, got '%s'", q.Function)
	}
}

func TestParser_WithLabels(t *testing.T) {
	p := NewParser()

	q, err := p.Parse(`cpu_usage{host="server1", region="us-east"}`)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if q.MetricName != "cpu_usage" {
		t.Errorf("Expected metric name 'cpu_usage', got '%s'", q.MetricName)
	}

	if len(q.Matchers) != 2 {
		t.Errorf("Expected 2 matchers, got %d", len(q.Matchers))
	}
}

func TestParser_WithFunction(t *testing.T) {
	p := NewParser()

	cases := []struct {
		input  string
		fn     string
		metric string
	}{
		{"sum(cpu_usage)", "sum", "cpu_usage"},
		{"avg(memory_used)", "avg", "memory_used"},
		{"max(disk_io)", "max", "disk_io"},
		{"min(requests)", "min", "requests"},
		{"delta(network_bytes)", "delta", "network_bytes"},
	}

	for _, tc := range cases {
		q, err := p.Parse(tc.input)
		if err != nil {
			t.Errorf("Parse '%s' failed: %v", tc.input, err)
			continue
		}

		if q.Function != tc.fn {
			t.Errorf("Expected function '%s', got '%s'", tc.fn, q.Function)
		}

		if q.MetricName != tc.metric {
			t.Errorf("Expected metric '%s', got '%s'", tc.metric, q.MetricName)
		}
	}
}

func TestParser_WithDuration(t *testing.T) {
	p := NewParser()

	cases := []struct {
		input   string
		durMins float64
	}{
		{"cpu_usage[5m]", 5},
		{"cpu_usage[1h]", 60},
		{"cpu_usage[24h]", 24 * 60},
		{"sum(cpu_usage[10m])", 10},
	}

	for _, tc := range cases {
		q, err := p.Parse(tc.input)
		if err != nil {
			t.Errorf("Parse '%s' failed: %v", tc.input, err)
			continue
		}

		durMins := q.Duration.Minutes()
		if durMins != tc.durMins {
			t.Errorf("Expected duration %.0f mins, got %.0f mins", tc.durMins, durMins)
		}
	}
}

func TestParser_ComplexQuery(t *testing.T) {
	p := NewParser()

	q, err := p.Parse(`avg(http_requests{method="GET", status=~"2.."}[5m])`)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if q.Function != "avg" {
		t.Errorf("Expected function 'avg', got '%s'", q.Function)
	}

	if q.MetricName != "http_requests" {
		t.Errorf("Expected metric 'http_requests', got '%s'", q.MetricName)
	}

	if len(q.Matchers) != 2 {
		t.Errorf("Expected 2 matchers, got %d", len(q.Matchers))
	}

	if q.Duration.Minutes() != 5 {
		t.Errorf("Expected 5 minute duration, got %.0f minutes", q.Duration.Minutes())
	}
}

func TestAggregations(t *testing.T) {
	samples := []model.Sample{
		{Timestamp: 1000, Value: 10},
		{Timestamp: 2000, Value: 20},
		{Timestamp: 3000, Value: 30},
		{Timestamp: 4000, Value: 40},
		{Timestamp: 5000, Value: 50},
	}

	tests := []struct {
		name     string
		fn       AggFunc
		expected float64
	}{
		{"sum", Sum, 150},
		{"avg", Avg, 30},
		{"max", Max, 50},
		{"min", Min, 10},
		{"delta", Delta, 40},
		{"count", Count, 5},
	}

	for _, tc := range tests {
		result := tc.fn(samples)
		if result != tc.expected {
			t.Errorf("%s: expected %f, got %f", tc.name, tc.expected, result)
		}
	}
}

func TestRate(t *testing.T) {
	// Counter that increases by 10 every second
	samples := []model.Sample{
		{Timestamp: 1000, Value: 100}, // 1s
		{Timestamp: 2000, Value: 110}, // 2s
		{Timestamp: 3000, Value: 120}, // 3s
		{Timestamp: 4000, Value: 130}, // 4s
		{Timestamp: 5000, Value: 140}, // 5s
	}

	rate := Rate(samples)

	// Delta is 40, time is 4 seconds, rate should be 10
	if rate != 10 {
		t.Errorf("Expected rate 10, got %f", rate)
	}
}

func TestPercentile(t *testing.T) {
	samples := []model.Sample{
		{Value: 10},
		{Value: 20},
		{Value: 30},
		{Value: 40},
		{Value: 50},
		{Value: 60},
		{Value: 70},
		{Value: 80},
		{Value: 90},
		{Value: 100},
	}

	tests := []struct {
		percentile float64
		expected   float64
	}{
		{0, 10},
		{50, 55},
		{100, 100},
	}

	for _, tc := range tests {
		result := Percentile(samples, tc.percentile)
		if result != tc.expected {
			t.Errorf("P%.0f: expected %f, got %f", tc.percentile, tc.expected, result)
		}
	}
}

func TestEmptySamples(t *testing.T) {
	empty := []model.Sample{}

	if Sum(empty) != 0 {
		t.Error("Sum of empty should be 0")
	}
	if Avg(empty) != 0 {
		t.Error("Avg of empty should be 0")
	}
	if Max(empty) != 0 {
		t.Error("Max of empty should be 0")
	}
	if Min(empty) != 0 {
		t.Error("Min of empty should be 0")
	}
	if Delta(empty) != 0 {
		t.Error("Delta of empty should be 0")
	}
	if Count(empty) != 0 {
		t.Error("Count of empty should be 0")
	}
}
