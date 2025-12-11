package storage

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"aetherdb/internal/model"
)

// MemTable is an in-memory buffer for recent time series data.
type MemTable struct {
	mu        sync.RWMutex
	series    map[uint64]*memSeries // series hash -> memSeries
	seriesIdx map[model.SeriesRef]*memSeries
	nextRef   atomic.Uint64
	minTime   int64
	maxTime   int64
	size      int64 // Approximate size in bytes
}

// memSeries holds samples for a single series in memory.
type memSeries struct {
	ref     model.SeriesRef
	series  *model.Series
	samples []model.Sample
	mu      sync.RWMutex
}

// NewMemTable creates a new MemTable.
func NewMemTable() *MemTable {
	return &MemTable{
		series:    make(map[uint64]*memSeries),
		seriesIdx: make(map[model.SeriesRef]*memSeries),
		minTime:   math.MaxInt64,
		maxTime:   math.MinInt64,
	}
}

// GetOrCreateSeries returns an existing series or creates a new one.
func (m *MemTable) GetOrCreateSeries(series *model.Series) (model.SeriesRef, bool) {
	hash := series.Hash()

	m.mu.RLock()
	ms, exists := m.series[hash]
	m.mu.RUnlock()

	if exists {
		return ms.ref, false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if ms, exists := m.series[hash]; exists {
		return ms.ref, false
	}

	ref := model.SeriesRef(m.nextRef.Add(1))
	ms = &memSeries{
		ref:     ref,
		series:  series,
		samples: make([]model.Sample, 0, 128),
	}

	m.series[hash] = ms
	m.seriesIdx[ref] = ms
	m.size += int64(64 + len(series.Name)) // Approximate

	return ref, true
}

// Append adds samples to a series.
func (m *MemTable) Append(ref model.SeriesRef, samples []model.Sample) error {
	m.mu.RLock()
	ms, exists := m.seriesIdx[ref]
	m.mu.RUnlock()

	if !exists {
		return nil // Series doesn't exist, ignore
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	for _, sample := range samples {
		ms.samples = append(ms.samples, sample)
		m.updateTimeRange(sample.Timestamp)
	}

	atomic.AddInt64(&m.size, int64(16*len(samples)))
	return nil
}

// updateTimeRange updates the min/max time range.
func (m *MemTable) updateTimeRange(ts int64) {
	for {
		current := atomic.LoadInt64(&m.minTime)
		if ts >= current {
			break
		}
		if atomic.CompareAndSwapInt64(&m.minTime, current, ts) {
			break
		}
	}

	for {
		current := atomic.LoadInt64(&m.maxTime)
		if ts <= current {
			break
		}
		if atomic.CompareAndSwapInt64(&m.maxTime, current, ts) {
			break
		}
	}
}

// Query returns samples for series matching the selector within the time range.
func (m *MemTable) Query(selector *model.MetricSelector, start, end int64) []model.QueryResult {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []model.QueryResult

	for _, ms := range m.series {
		if !selector.Matches(ms.series) {
			continue
		}

		ms.mu.RLock()
		samples := m.filterSamples(ms.samples, start, end)
		ms.mu.RUnlock()

		if len(samples) > 0 {
			results = append(results, model.QueryResult{
				Series:  ms.series,
				Samples: samples,
			})
		}
	}

	return results
}

// filterSamples returns samples within the time range.
func (m *MemTable) filterSamples(samples []model.Sample, start, end int64) []model.Sample {
	// Find start index using binary search
	startIdx := sort.Search(len(samples), func(i int) bool {
		return samples[i].Timestamp >= start
	})

	if startIdx >= len(samples) {
		return nil
	}

	// Find end index
	endIdx := sort.Search(len(samples), func(i int) bool {
		return samples[i].Timestamp > end
	})

	if startIdx >= endIdx {
		return nil
	}

	result := make([]model.Sample, endIdx-startIdx)
	copy(result, samples[startIdx:endIdx])
	return result
}

// GetAllSeries returns all series in the memtable.
func (m *MemTable) GetAllSeries() []*model.Series {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*model.Series, 0, len(m.series))
	for _, ms := range m.series {
		result = append(result, ms.series)
	}
	return result
}

// GetSeriesByRef returns a series by its reference.
func (m *MemTable) GetSeriesByRef(ref model.SeriesRef) *model.Series {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if ms, exists := m.seriesIdx[ref]; exists {
		return ms.series
	}
	return nil
}

// Snapshot returns all data for compaction.
func (m *MemTable) Snapshot() []memSeriesSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]memSeriesSnapshot, 0, len(m.series))
	for _, ms := range m.series {
		ms.mu.RLock()
		samples := make([]model.Sample, len(ms.samples))
		copy(samples, ms.samples)
		ms.mu.RUnlock()

		if len(samples) > 0 {
			sort.Sort(model.Samples(samples))
			result = append(result, memSeriesSnapshot{
				series:  ms.series,
				samples: samples,
			})
		}
	}

	return result
}

// memSeriesSnapshot holds a point-in-time snapshot of a series.
type memSeriesSnapshot struct {
	series  *model.Series
	samples []model.Sample
}

// Size returns the approximate size of the memtable in bytes.
func (m *MemTable) Size() int64 {
	return atomic.LoadInt64(&m.size)
}

// TimeRange returns the min and max timestamps in the memtable.
func (m *MemTable) TimeRange() (int64, int64) {
	min := atomic.LoadInt64(&m.minTime)
	max := atomic.LoadInt64(&m.maxTime)
	if min == math.MaxInt64 {
		return 0, 0
	}
	return min, max
}

// Clear resets the memtable.
func (m *MemTable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.series = make(map[uint64]*memSeries)
	m.seriesIdx = make(map[model.SeriesRef]*memSeries)
	atomic.StoreInt64(&m.minTime, math.MaxInt64)
	atomic.StoreInt64(&m.maxTime, math.MinInt64)
	atomic.StoreInt64(&m.size, 0)
}

// SeriesCount returns the number of series in the memtable.
func (m *MemTable) SeriesCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.series)
}
