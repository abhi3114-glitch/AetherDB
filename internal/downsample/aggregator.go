// Package downsample implements the downsampling engine for AetherDB.
package downsample

import (
	"time"

	"aetherdb/internal/model"
)

// Resolution represents a downsampling resolution.
type Resolution time.Duration

// Standard resolutions
const (
	Resolution1m = Resolution(1 * time.Minute)
	Resolution5m = Resolution(5 * time.Minute)
	Resolution1h = Resolution(1 * time.Hour)
)

// AllResolutions returns all supported resolutions in order.
func AllResolutions() []Resolution {
	return []Resolution{Resolution1m, Resolution5m, Resolution1h}
}

// String returns the string representation of a resolution.
func (r Resolution) String() string {
	d := time.Duration(r)
	switch d {
	case 1 * time.Minute:
		return "1m"
	case 5 * time.Minute:
		return "5m"
	case 1 * time.Hour:
		return "1h"
	default:
		return d.String()
	}
}

// Milliseconds returns the resolution in milliseconds.
func (r Resolution) Milliseconds() int64 {
	return time.Duration(r).Milliseconds()
}

// DownsampledPoint holds pre-aggregated data for a single time bucket.
type DownsampledPoint struct {
	Timestamp int64   // Start of the bucket (aligned to resolution)
	Min       float64 // Minimum value in bucket
	Max       float64 // Maximum value in bucket
	Sum       float64 // Sum of all values
	Count     int64   // Number of samples
	First     float64 // First value in bucket
	Last      float64 // Last value in bucket
}

// Avg returns the average value in the bucket.
func (p *DownsampledPoint) Avg() float64 {
	if p.Count == 0 {
		return 0
	}
	return p.Sum / float64(p.Count)
}

// DownsampledSeries holds downsampled data for a single series.
type DownsampledSeries struct {
	Series     *model.Series
	Resolution Resolution
	Points     []DownsampledPoint
}

// Aggregator performs downsampling aggregation on raw samples.
type Aggregator struct {
	resolutions []Resolution
}

// NewAggregator creates a new Aggregator with the given resolutions.
func NewAggregator(resolutions []Resolution) *Aggregator {
	if len(resolutions) == 0 {
		resolutions = AllResolutions()
	}
	return &Aggregator{
		resolutions: resolutions,
	}
}

// Downsample aggregates samples into downsampled points for a given resolution.
func (a *Aggregator) Downsample(samples []model.Sample, resolution Resolution) []DownsampledPoint {
	if len(samples) == 0 {
		return nil
	}

	resMs := resolution.Milliseconds()

	// Group samples by bucket
	buckets := make(map[int64][]model.Sample)

	for _, s := range samples {
		// Align to bucket start
		bucket := (s.Timestamp / resMs) * resMs
		buckets[bucket] = append(buckets[bucket], s)
	}

	// Aggregate each bucket
	points := make([]DownsampledPoint, 0, len(buckets))

	for bucket, bSamples := range buckets {
		point := aggregateBucket(bucket, bSamples)
		points = append(points, point)
	}

	// Sort by timestamp
	sortPointsByTimestamp(points)

	return points
}

// DownsampleAll downsamples to all configured resolutions.
func (a *Aggregator) DownsampleAll(samples []model.Sample) map[Resolution][]DownsampledPoint {
	result := make(map[Resolution][]DownsampledPoint)

	for _, res := range a.resolutions {
		result[res] = a.Downsample(samples, res)
	}

	return result
}

// aggregateBucket creates a downsampled point from samples in a bucket.
func aggregateBucket(timestamp int64, samples []model.Sample) DownsampledPoint {
	if len(samples) == 0 {
		return DownsampledPoint{Timestamp: timestamp}
	}

	point := DownsampledPoint{
		Timestamp: timestamp,
		Min:       samples[0].Value,
		Max:       samples[0].Value,
		Sum:       0,
		Count:     int64(len(samples)),
		First:     samples[0].Value,
		Last:      samples[len(samples)-1].Value,
	}

	for _, s := range samples {
		if s.Value < point.Min {
			point.Min = s.Value
		}
		if s.Value > point.Max {
			point.Max = s.Value
		}
		point.Sum += s.Value
	}

	return point
}

// sortPointsByTimestamp sorts points by timestamp.
func sortPointsByTimestamp(points []DownsampledPoint) {
	// Simple bubble sort for small slices
	n := len(points)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if points[j].Timestamp > points[j+1].Timestamp {
				points[j], points[j+1] = points[j+1], points[j]
			}
		}
	}
}

// ReconstructSamples creates synthetic samples from downsampled points.
// This is useful for queries that need sample-based aggregation.
func ReconstructSamples(points []DownsampledPoint, useAvg bool) []model.Sample {
	samples := make([]model.Sample, len(points))

	for i, p := range points {
		var value float64
		if useAvg {
			value = p.Avg()
		} else {
			value = p.Last
		}

		samples[i] = model.Sample{
			Timestamp: p.Timestamp,
			Value:     value,
		}
	}

	return samples
}

// MergePoints merges overlapping downsampled points.
func MergePoints(a, b []DownsampledPoint) []DownsampledPoint {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}

	// Merge by timestamp
	merged := make(map[int64]DownsampledPoint)

	for _, p := range a {
		merged[p.Timestamp] = p
	}

	for _, p := range b {
		if existing, ok := merged[p.Timestamp]; ok {
			// Merge the two points
			m := DownsampledPoint{
				Timestamp: p.Timestamp,
				Min:       min(existing.Min, p.Min),
				Max:       max(existing.Max, p.Max),
				Sum:       existing.Sum + p.Sum,
				Count:     existing.Count + p.Count,
				First:     existing.First, // Keep first from earlier data
				Last:      p.Last,         // Keep last from later data
			}
			merged[p.Timestamp] = m
		} else {
			merged[p.Timestamp] = p
		}
	}

	// Convert to slice and sort
	result := make([]DownsampledPoint, 0, len(merged))
	for _, p := range merged {
		result = append(result, p)
	}
	sortPointsByTimestamp(result)

	return result
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
