package query

import (
	"math"
	"sort"

	"aetherdb/internal/model"
)

// AggFunc is the signature for aggregation functions.
type AggFunc func(samples []model.Sample) float64

// Aggregations maps function names to their implementations.
var Aggregations = map[string]AggFunc{
	"sum":   Sum,
	"avg":   Avg,
	"max":   Max,
	"min":   Min,
	"delta": Delta,
	"count": Count,
	"rate":  Rate,
}

// Sum returns the sum of all sample values.
func Sum(samples []model.Sample) float64 {
	if len(samples) == 0 {
		return 0
	}
	var sum float64
	for _, s := range samples {
		sum += s.Value
	}
	return sum
}

// Avg returns the average of all sample values.
func Avg(samples []model.Sample) float64 {
	if len(samples) == 0 {
		return 0
	}
	return Sum(samples) / float64(len(samples))
}

// Max returns the maximum sample value.
func Max(samples []model.Sample) float64 {
	if len(samples) == 0 {
		return 0
	}
	max := samples[0].Value
	for _, s := range samples[1:] {
		if s.Value > max {
			max = s.Value
		}
	}
	return max
}

// Min returns the minimum sample value.
func Min(samples []model.Sample) float64 {
	if len(samples) == 0 {
		return 0
	}
	min := samples[0].Value
	for _, s := range samples[1:] {
		if s.Value < min {
			min = s.Value
		}
	}
	return min
}

// Delta returns the difference between the last and first values.
// This is useful for counter metrics to see the total increase.
func Delta(samples []model.Sample) float64 {
	if len(samples) < 2 {
		return 0
	}

	// Sort by timestamp
	sorted := make([]model.Sample, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp < sorted[j].Timestamp
	})

	return sorted[len(sorted)-1].Value - sorted[0].Value
}

// Count returns the number of samples.
func Count(samples []model.Sample) float64 {
	return float64(len(samples))
}

// Rate returns the per-second rate of change.
// Calculated as delta / time_range_seconds.
func Rate(samples []model.Sample) float64 {
	if len(samples) < 2 {
		return 0
	}

	// Sort by timestamp
	sorted := make([]model.Sample, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp < sorted[j].Timestamp
	})

	first := sorted[0]
	last := sorted[len(sorted)-1]

	deltaValue := last.Value - first.Value
	deltaTime := float64(last.Timestamp-first.Timestamp) / 1000.0 // Convert ms to seconds

	if deltaTime == 0 {
		return 0
	}

	return deltaValue / deltaTime
}

// Percentile returns the p-th percentile of sample values.
func Percentile(samples []model.Sample, p float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	if p < 0 || p > 100 {
		return 0
	}

	values := make([]float64, len(samples))
	for i, s := range samples {
		values[i] = s.Value
	}
	sort.Float64s(values)

	idx := (p / 100.0) * float64(len(values)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))

	if lower == upper {
		return values[lower]
	}

	// Linear interpolation
	weight := idx - float64(lower)
	return values[lower]*(1-weight) + values[upper]*weight
}

// StdDev returns the standard deviation of sample values.
func StdDev(samples []model.Sample) float64 {
	if len(samples) < 2 {
		return 0
	}

	avg := Avg(samples)
	var sumSquares float64
	for _, s := range samples {
		diff := s.Value - avg
		sumSquares += diff * diff
	}

	return math.Sqrt(sumSquares / float64(len(samples)))
}

// Variance returns the variance of sample values.
func Variance(samples []model.Sample) float64 {
	if len(samples) < 2 {
		return 0
	}

	avg := Avg(samples)
	var sumSquares float64
	for _, s := range samples {
		diff := s.Value - avg
		sumSquares += diff * diff
	}

	return sumSquares / float64(len(samples))
}

// First returns the value of the first sample by timestamp.
func First(samples []model.Sample) float64 {
	if len(samples) == 0 {
		return 0
	}

	sorted := make([]model.Sample, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp < sorted[j].Timestamp
	})

	return sorted[0].Value
}

// Last returns the value of the last sample by timestamp.
func Last(samples []model.Sample) float64 {
	if len(samples) == 0 {
		return 0
	}

	sorted := make([]model.Sample, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp < sorted[j].Timestamp
	})

	return sorted[len(sorted)-1].Value
}
