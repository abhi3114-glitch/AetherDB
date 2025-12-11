package downsample

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"aetherdb/internal/model"
	"aetherdb/internal/storage"
)

// Scheduler manages background downsampling jobs.
type Scheduler struct {
	engine      *storage.Engine
	aggregator  *Aggregator
	dataDir     string
	resolutions []Resolution

	mu             sync.Mutex
	lastDownsample map[string]int64 // block ULID -> last downsampled timestamp

	closeCh chan struct{}
	wg      sync.WaitGroup
}

// NewScheduler creates a new downsampling scheduler.
func NewScheduler(engine *storage.Engine, dataDir string) *Scheduler {
	return &Scheduler{
		engine:         engine,
		aggregator:     NewAggregator(nil),
		dataDir:        dataDir,
		resolutions:    AllResolutions(),
		lastDownsample: make(map[string]int64),
		closeCh:        make(chan struct{}),
	}
}

// Start begins the background downsampling process.
func (s *Scheduler) Start() {
	s.wg.Add(1)
	go s.runLoop()
}

// Stop stops the scheduler.
func (s *Scheduler) Stop() {
	close(s.closeCh)
	s.wg.Wait()
}

// runLoop periodically checks for blocks to downsample.
func (s *Scheduler) runLoop() {
	defer s.wg.Done()

	// Run immediately on start
	s.processBlocks()

	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.closeCh:
			return
		case <-ticker.C:
			s.processBlocks()
		}
	}
}

// processBlocks checks all blocks and downsamples as needed.
func (s *Scheduler) processBlocks() {
	blocks := s.engine.ListBlocks()

	for _, block := range blocks {
		meta := block.Meta()

		// Skip if already downsampled
		s.mu.Lock()
		lastTs := s.lastDownsample[meta.ULID]
		s.mu.Unlock()

		if lastTs >= meta.MaxTime {
			continue
		}

		// Downsample this block
		if err := s.downsampleBlock(block, meta); err != nil {
			fmt.Printf("Error downsampling block %s: %v\n", meta.ULID, err)
			continue
		}

		s.mu.Lock()
		s.lastDownsample[meta.ULID] = meta.MaxTime
		s.mu.Unlock()
	}
}

// downsampleBlock creates downsampled data for a block.
func (s *Scheduler) downsampleBlock(block *storage.Block, meta storage.BlockMeta) error {
	// Read all series from the block
	selector := model.NewMetricSelector("", nil) // Match all
	results, err := block.Query(selector, meta.MinTime, meta.MaxTime)
	if err != nil {
		return fmt.Errorf("query block: %w", err)
	}

	// Downsample each series
	for _, res := range s.resolutions {
		resDir := s.resolutionDir(meta.ULID, res)
		if err := os.MkdirAll(resDir, 0755); err != nil {
			return fmt.Errorf("create resolution dir: %w", err)
		}

		for _, result := range results {
			points := s.aggregator.Downsample(result.Samples, res)
			if len(points) == 0 {
				continue
			}

			// Save downsampled data
			dsData := &DownsampledSeriesData{
				SeriesHash: result.Series.Hash(),
				SeriesName: result.Series.Name,
				Labels:     result.Series.Labels,
				Resolution: res.String(),
				Points:     points,
			}

			if err := s.saveDownsampledData(resDir, dsData); err != nil {
				return fmt.Errorf("save downsampled data: %w", err)
			}
		}
	}

	return nil
}

// DownsampledSeriesData is the on-disk format for downsampled data.
type DownsampledSeriesData struct {
	SeriesHash uint64             `json:"seriesHash"`
	SeriesName string             `json:"seriesName"`
	Labels     map[string]string  `json:"labels"`
	Resolution string             `json:"resolution"`
	Points     []DownsampledPoint `json:"points"`
}

// resolutionDir returns the directory for a block's resolution data.
func (s *Scheduler) resolutionDir(blockULID string, res Resolution) string {
	return filepath.Join(s.dataDir, "downsample", blockULID, res.String())
}

// saveDownsampledData saves downsampled data to disk.
func (s *Scheduler) saveDownsampledData(dir string, data *DownsampledSeriesData) error {
	filename := fmt.Sprintf("%016x.json", data.SeriesHash)
	path := filepath.Join(dir, filename)

	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return os.WriteFile(path, jsonData, 0644)
}

// loadDownsampledData loads downsampled data from disk.
func (s *Scheduler) loadDownsampledData(path string) (*DownsampledSeriesData, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var dsData DownsampledSeriesData
	if err := json.Unmarshal(data, &dsData); err != nil {
		return nil, err
	}

	return &dsData, nil
}

// QueryDownsampled queries downsampled data for a series at a given resolution.
func (s *Scheduler) QueryDownsampled(
	selector *model.MetricSelector,
	start, end int64,
	resolution Resolution,
) ([]DownsampledSeries, error) {
	blocks := s.engine.ListBlocks()
	var results []DownsampledSeries

	for _, block := range blocks {
		meta := block.Meta()

		// Check if block overlaps with time range
		if meta.MinTime > end || meta.MaxTime < start {
			continue
		}

		// Look for downsampled data
		resDir := s.resolutionDir(meta.ULID, resolution)
		entries, err := os.ReadDir(resDir)
		if err != nil {
			continue // No downsampled data yet
		}

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}

			path := filepath.Join(resDir, entry.Name())
			dsData, err := s.loadDownsampledData(path)
			if err != nil {
				continue
			}

			// Check if series matches selector
			series := model.NewSeries(dsData.SeriesName, dsData.Labels)
			if !selector.Matches(series) {
				continue
			}

			// Filter points by time range
			var filteredPoints []DownsampledPoint
			for _, p := range dsData.Points {
				if p.Timestamp >= start && p.Timestamp <= end {
					filteredPoints = append(filteredPoints, p)
				}
			}

			if len(filteredPoints) > 0 {
				results = append(results, DownsampledSeries{
					Series:     series,
					Resolution: resolution,
					Points:     filteredPoints,
				})
			}
		}
	}

	return results, nil
}

// ForceDownsample forces downsampling of all blocks.
func (s *Scheduler) ForceDownsample() error {
	s.mu.Lock()
	s.lastDownsample = make(map[string]int64)
	s.mu.Unlock()

	s.processBlocks()
	return nil
}

// SelectResolution automatically selects the best resolution for a query.
func SelectResolution(start, end int64) Resolution {
	duration := time.Duration(end-start) * time.Millisecond

	switch {
	case duration >= 24*time.Hour:
		return Resolution1h
	case duration >= 6*time.Hour:
		return Resolution5m
	case duration >= 1*time.Hour:
		return Resolution1m
	default:
		return 0 // Use raw data
	}
}
