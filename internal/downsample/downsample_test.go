package downsample

import (
	"testing"
	"time"

	"aetherdb/internal/model"
)

func TestDownsample_1Minute(t *testing.T) {
	agg := NewAggregator(nil)

	// Generate samples over 5 minutes (1 per second)
	var samples []model.Sample
	baseTime := int64(0)
	for i := 0; i < 300; i++ { // 5 minutes
		samples = append(samples, model.Sample{
			Timestamp: baseTime + int64(i*1000),
			Value:     float64(i),
		})
	}

	points := agg.Downsample(samples, Resolution1m)

	// Should have 5 buckets (0-1m, 1-2m, 2-3m, 3-4m, 4-5m)
	if len(points) != 5 {
		t.Errorf("Expected 5 buckets, got %d", len(points))
	}

	// First bucket should have 60 samples (0-59)
	if points[0].Count != 60 {
		t.Errorf("Expected 60 samples in first bucket, got %d", points[0].Count)
	}

	// First bucket min should be 0
	if points[0].Min != 0 {
		t.Errorf("Expected min 0, got %f", points[0].Min)
	}

	// First bucket max should be 59
	if points[0].Max != 59 {
		t.Errorf("Expected max 59, got %f", points[0].Max)
	}
}

func TestDownsample_AllResolutions(t *testing.T) {
	agg := NewAggregator(nil)

	// Generate 2 hours of data (1 sample per minute)
	var samples []model.Sample
	baseTime := int64(0)
	for i := 0; i < 120; i++ {
		samples = append(samples, model.Sample{
			Timestamp: baseTime + int64(i*60000), // Every minute
			Value:     float64(i),
		})
	}

	result := agg.DownsampleAll(samples)

	// 1m resolution: 120 buckets
	if len(result[Resolution1m]) != 120 {
		t.Errorf("1m: expected 120 buckets, got %d", len(result[Resolution1m]))
	}

	// 5m resolution: 24 buckets
	if len(result[Resolution5m]) != 24 {
		t.Errorf("5m: expected 24 buckets, got %d", len(result[Resolution5m]))
	}

	// 1h resolution: 2 buckets
	if len(result[Resolution1h]) != 2 {
		t.Errorf("1h: expected 2 buckets, got %d", len(result[Resolution1h]))
	}
}

func TestDownsampledPoint_Avg(t *testing.T) {
	point := DownsampledPoint{
		Sum:   100,
		Count: 10,
	}

	if point.Avg() != 10 {
		t.Errorf("Expected avg 10, got %f", point.Avg())
	}

	// Test zero count
	zeroPoint := DownsampledPoint{
		Sum:   100,
		Count: 0,
	}

	if zeroPoint.Avg() != 0 {
		t.Errorf("Expected avg 0 for zero count, got %f", zeroPoint.Avg())
	}
}

func TestReconstructSamples(t *testing.T) {
	points := []DownsampledPoint{
		{Timestamp: 0, Sum: 100, Count: 10, Last: 15},
		{Timestamp: 60000, Sum: 200, Count: 10, Last: 25},
		{Timestamp: 120000, Sum: 300, Count: 10, Last: 35},
	}

	// Reconstruct using average
	avgSamples := ReconstructSamples(points, true)
	if avgSamples[0].Value != 10 {
		t.Errorf("Expected value 10 (avg), got %f", avgSamples[0].Value)
	}

	// Reconstruct using last
	lastSamples := ReconstructSamples(points, false)
	if lastSamples[0].Value != 15 {
		t.Errorf("Expected value 15 (last), got %f", lastSamples[0].Value)
	}
}

func TestMergePoints(t *testing.T) {
	a := []DownsampledPoint{
		{Timestamp: 0, Min: 10, Max: 20, Sum: 100, Count: 10},
		{Timestamp: 60000, Min: 15, Max: 25, Sum: 200, Count: 10},
	}

	b := []DownsampledPoint{
		{Timestamp: 60000, Min: 12, Max: 28, Sum: 150, Count: 8},
		{Timestamp: 120000, Min: 20, Max: 30, Sum: 250, Count: 10},
	}

	merged := MergePoints(a, b)

	if len(merged) != 3 {
		t.Fatalf("Expected 3 merged points, got %d", len(merged))
	}

	// Check merged point at 60000
	var merged60 *DownsampledPoint
	for i := range merged {
		if merged[i].Timestamp == 60000 {
			merged60 = &merged[i]
			break
		}
	}

	if merged60 == nil {
		t.Fatal("Missing merged point at 60000")
	}

	if merged60.Min != 12 {
		t.Errorf("Expected merged min 12, got %f", merged60.Min)
	}

	if merged60.Max != 28 {
		t.Errorf("Expected merged max 28, got %f", merged60.Max)
	}

	if merged60.Sum != 350 {
		t.Errorf("Expected merged sum 350, got %f", merged60.Sum)
	}

	if merged60.Count != 18 {
		t.Errorf("Expected merged count 18, got %d", merged60.Count)
	}
}

func TestSelectResolution(t *testing.T) {
	tests := []struct {
		duration time.Duration
		expected Resolution
	}{
		{30 * time.Minute, 0},          // Raw data
		{2 * time.Hour, Resolution1m},  // 1m
		{12 * time.Hour, Resolution5m}, // 5m
		{48 * time.Hour, Resolution1h}, // 1h
	}

	for _, tc := range tests {
		end := time.Now().UnixMilli()
		start := end - tc.duration.Milliseconds()
		result := SelectResolution(start, end)

		if result != tc.expected {
			t.Errorf("Duration %v: expected resolution %v, got %v",
				tc.duration, tc.expected, result)
		}
	}
}

func TestResolution_String(t *testing.T) {
	tests := []struct {
		res      Resolution
		expected string
	}{
		{Resolution1m, "1m"},
		{Resolution5m, "5m"},
		{Resolution1h, "1h"},
	}

	for _, tc := range tests {
		if tc.res.String() != tc.expected {
			t.Errorf("Expected %s, got %s", tc.expected, tc.res.String())
		}
	}
}
