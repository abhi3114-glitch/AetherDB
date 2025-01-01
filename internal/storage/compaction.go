package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"aetherdb/internal/model"
)

const (
	// Compaction levels and their block sizes
	Level0BlockDuration = 2 * time.Hour  // Raw data blocks
	Level1BlockDuration = 6 * time.Hour  // Compact 3 L0 → 1 L1
	Level2BlockDuration = 24 * time.Hour // Compact 4 L1 → 1 L2
)

// Compactor handles block compaction to merge smaller blocks into larger ones.
type Compactor struct {
	db *Engine
	mu sync.Mutex
}

// NewCompactor creates a new Compactor.
func NewCompactor(db *Engine) *Compactor {
	return &Compactor{db: db}
}

// CompactionPlan describes which blocks to compact together.
type CompactionPlan struct {
	Level       int
	SourceDirs  []string
	TargetLevel int
}

// Plan returns a list of compaction plans.
func (c *Compactor) Plan() ([]CompactionPlan, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	blocks := c.db.ListBlocks()
	if len(blocks) == 0 {
		return nil, nil
	}

	var plans []CompactionPlan

	// Group blocks by level
	level0 := make([]*Block, 0)
	level1 := make([]*Block, 0)

	for _, b := range blocks {
		switch b.meta.Compaction {
		case 0:
			level0 = append(level0, b)
		case 1:
			level1 = append(level1, b)
		}
	}

	// Sort by time
	sort.Slice(level0, func(i, j int) bool {
		return level0[i].meta.MinTime < level0[j].meta.MinTime
	})
	sort.Slice(level1, func(i, j int) bool {
		return level1[i].meta.MinTime < level1[j].meta.MinTime
	})

	// Plan L0 → L1 compaction (3 blocks)
	if len(level0) >= 3 {
		dirs := make([]string, 3)
		for i := 0; i < 3; i++ {
			dirs[i] = level0[i].dir
		}
		plans = append(plans, CompactionPlan{
			Level:       0,
			SourceDirs:  dirs,
			TargetLevel: 1,
		})
	}

	// Plan L1 → L2 compaction (4 blocks)
	if len(level1) >= 4 {
		dirs := make([]string, 4)
		for i := 0; i < 4; i++ {
			dirs[i] = level1[i].dir
		}
		plans = append(plans, CompactionPlan{
			Level:       1,
			SourceDirs:  dirs,
			TargetLevel: 2,
		})
	}

	return plans, nil
}

// Compact executes a compaction plan.
func (c *Compactor) Compact(plan CompactionPlan) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Open source blocks
	sourceBlocks := make([]*Block, len(plan.SourceDirs))
	for i, dir := range plan.SourceDirs {
		block, err := OpenBlock(dir)
		if err != nil {
			return "", fmt.Errorf("open block %s: %w", dir, err)
		}
		sourceBlocks[i] = block
	}

	// Determine time range
	var minTime int64 = sourceBlocks[0].meta.MinTime
	var maxTime int64 = sourceBlocks[0].meta.MaxTime
	for _, b := range sourceBlocks[1:] {
		if b.meta.MinTime < minTime {
			minTime = b.meta.MinTime
		}
		if b.meta.MaxTime > maxTime {
			maxTime = b.meta.MaxTime
		}
	}

	// Create output block
	outDir := filepath.Join(c.db.blocksDir, fmt.Sprintf("block_%s", generateULID()))
	writer, err := NewBlockWriter(outDir, minTime, maxTime, plan.TargetLevel)
	if err != nil {
		return "", fmt.Errorf("create block writer: %w", err)
	}

	// Merge all series from source blocks
	mergedSeries := make(map[uint64]*mergedSeriesData)

	for _, block := range sourceBlocks {
		for _, entry := range block.index.series {
			hash := entry.series.Hash()

			samples, err := block.chunks.ReadSamples(entry.chunkOffset, entry.chunkLength, minTime, maxTime)
			if err != nil {
				writer.Cancel()
				return "", fmt.Errorf("read samples: %w", err)
			}

			if existing, ok := mergedSeries[hash]; ok {
				existing.samples = append(existing.samples, samples...)
			} else {
				mergedSeries[hash] = &mergedSeriesData{
					series:  entry.series,
					samples: samples,
				}
			}
		}
	}

	// Sort samples and write to output block
	for _, data := range mergedSeries {
		sort.Sort(model.Samples(data.samples))

		// Remove duplicates
		data.samples = deduplicateSamples(data.samples)

		if err := writer.AddSeries(data.series, data.samples); err != nil {
			writer.Cancel()
			return "", fmt.Errorf("add series: %w", err)
		}
	}

	if err := writer.Finish(); err != nil {
		writer.Cancel()
		return "", fmt.Errorf("finish block: %w", err)
	}

	// Close source blocks
	for _, b := range sourceBlocks {
		b.Close()
	}

	// Remove source blocks
	for _, dir := range plan.SourceDirs {
		if err := os.RemoveAll(dir); err != nil {
			return outDir, fmt.Errorf("remove source block %s: %w", dir, err)
		}
	}

	// Reload blocks in engine
	c.db.reloadBlocks()

	return outDir, nil
}

type mergedSeriesData struct {
	series  *model.Series
	samples []model.Sample
}

// deduplicateSamples removes duplicate timestamps, keeping the last value.
func deduplicateSamples(samples []model.Sample) []model.Sample {
	if len(samples) <= 1 {
		return samples
	}

	result := make([]model.Sample, 0, len(samples))
	result = append(result, samples[0])

	for i := 1; i < len(samples); i++ {
		if samples[i].Timestamp != samples[i-1].Timestamp {
			result = append(result, samples[i])
		} else {
			// Keep last value for duplicate timestamp
			result[len(result)-1] = samples[i]
		}
	}

	return result
}

// CompactMemTable compacts the current memtable to a block.
func (c *Compactor) CompactMemTable(mt *MemTable, walSegment int) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	snapshot := mt.Snapshot()
	if len(snapshot) == 0 {
		return "", nil
	}

	minTime, maxTime := mt.TimeRange()

	// Create block
	outDir := filepath.Join(c.db.blocksDir, fmt.Sprintf("block_%s", generateULID()))
	writer, err := NewBlockWriter(outDir, minTime, maxTime, 0)
	if err != nil {
		return "", fmt.Errorf("create block writer: %w", err)
	}

	for _, ss := range snapshot {
		if err := writer.AddSeries(ss.series, ss.samples); err != nil {
			writer.Cancel()
			return "", fmt.Errorf("add series: %w", err)
		}
	}

	if err := writer.Finish(); err != nil {
		writer.Cancel()
		return "", fmt.Errorf("finish block: %w", err)
	}

	// Reload blocks in engine
	c.db.reloadBlocks()

	return outDir, nil
}
