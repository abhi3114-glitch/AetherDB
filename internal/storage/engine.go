package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"aetherdb/internal/model"
)

const (
	defaultMemTableSize = 64 * 1024 * 1024 // 64MB
	defaultFlushPeriod  = 2 * time.Minute
)

// Engine is the main storage engine that coordinates all storage components.
type Engine struct {
	dataDir   string
	walDir    string
	blocksDir string

	wal      *WAL
	memtable *MemTable
	blocks   []*Block
	blocksMu sync.RWMutex

	compactor *Compactor

	maxMemTableSize int64
	flushPeriod     time.Duration

	seriesIndex map[uint64]model.SeriesRef // Global series hash -> ref
	seriesMu    sync.RWMutex

	closeCh chan struct{}
	wg      sync.WaitGroup
}

// EngineConfig holds configuration for the storage engine.
type EngineConfig struct {
	DataDir         string
	MaxMemTableSize int64
	FlushPeriod     time.Duration
}

// NewEngine creates a new storage engine.
func NewEngine(cfg EngineConfig) (*Engine, error) {
	if cfg.MaxMemTableSize == 0 {
		cfg.MaxMemTableSize = defaultMemTableSize
	}
	if cfg.FlushPeriod == 0 {
		cfg.FlushPeriod = defaultFlushPeriod
	}

	walDir := filepath.Join(cfg.DataDir, "wal")
	blocksDir := filepath.Join(cfg.DataDir, "blocks")

	// Create directories
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("create WAL dir: %w", err)
	}
	if err := os.MkdirAll(blocksDir, 0755); err != nil {
		return nil, fmt.Errorf("create blocks dir: %w", err)
	}

	// Initialize WAL
	wal, err := NewWAL(walDir)
	if err != nil {
		return nil, fmt.Errorf("create WAL: %w", err)
	}

	e := &Engine{
		dataDir:         cfg.DataDir,
		walDir:          walDir,
		blocksDir:       blocksDir,
		wal:             wal,
		memtable:        NewMemTable(),
		blocks:          make([]*Block, 0),
		maxMemTableSize: cfg.MaxMemTableSize,
		flushPeriod:     cfg.FlushPeriod,
		seriesIndex:     make(map[uint64]model.SeriesRef),
		closeCh:         make(chan struct{}),
	}

	e.compactor = NewCompactor(e)

	// Load existing blocks
	if err := e.loadBlocks(); err != nil {
		return nil, fmt.Errorf("load blocks: %w", err)
	}

	// Replay WAL
	if err := e.replayWAL(); err != nil {
		return nil, fmt.Errorf("replay WAL: %w", err)
	}

	// Start background jobs
	e.wg.Add(2)
	go e.flushLoop()
	go e.compactionLoop()

	return e, nil
}

// loadBlocks loads all existing blocks from disk.
func (e *Engine) loadBlocks() error {
	entries, err := os.ReadDir(e.blocksDir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "block_") {
			continue
		}

		blockDir := filepath.Join(e.blocksDir, entry.Name())
		block, err := OpenBlock(blockDir)
		if err != nil {
			// Log and skip corrupt blocks
			fmt.Printf("Warning: failed to open block %s: %v\n", blockDir, err)
			continue
		}

		e.blocks = append(e.blocks, block)
	}

	return nil
}

// replayWAL replays WAL entries to recover state.
func (e *Engine) replayWAL() error {
	return e.wal.Replay(func(entry WALEntry) error {
		if entry.Series != nil {
			// Re-register series
			ref, _ := e.memtable.GetOrCreateSeries(entry.Series)
			e.seriesMu.Lock()
			e.seriesIndex[entry.Series.Hash()] = ref
			e.seriesMu.Unlock()
		} else {
			// Add samples
			e.memtable.Append(entry.SeriesRef, entry.Samples)
		}
		return nil
	})
}

// reloadBlocks reloads blocks after compaction.
func (e *Engine) reloadBlocks() {
	e.blocksMu.Lock()
	defer e.blocksMu.Unlock()

	// Close existing blocks
	for _, b := range e.blocks {
		b.Close()
	}
	e.blocks = nil

	// Reload
	e.loadBlocks()
}

// Write writes samples for a series.
func (e *Engine) Write(series *model.Series, samples []model.Sample) error {
	hash := series.Hash()

	// Get or create series ref
	e.seriesMu.RLock()
	ref, exists := e.seriesIndex[hash]
	e.seriesMu.RUnlock()

	var walEntry WALEntry

	if !exists {
		// Create new series
		ref, _ = e.memtable.GetOrCreateSeries(series)

		e.seriesMu.Lock()
		e.seriesIndex[hash] = ref
		e.seriesMu.Unlock()

		walEntry = WALEntry{
			SeriesRef: ref,
			Series:    series,
			Samples:   samples,
		}
	} else {
		walEntry = WALEntry{
			SeriesRef: ref,
			Samples:   samples,
		}
	}

	// Write to WAL
	if err := e.wal.Append([]WALEntry{walEntry}); err != nil {
		return fmt.Errorf("WAL append: %w", err)
	}

	// Write to memtable
	if err := e.memtable.Append(ref, samples); err != nil {
		return fmt.Errorf("memtable append: %w", err)
	}

	return nil
}

// Query queries samples for series matching the selector within the time range.
func (e *Engine) Query(selector *model.MetricSelector, start, end int64) ([]model.QueryResult, error) {
	var results []model.QueryResult

	// Query memtable
	memResults := e.memtable.Query(selector, start, end)
	results = append(results, memResults...)

	// Query blocks
	e.blocksMu.RLock()
	blocks := make([]*Block, len(e.blocks))
	copy(blocks, e.blocks)
	e.blocksMu.RUnlock()

	for _, block := range blocks {
		blockResults, err := block.Query(selector, start, end)
		if err != nil {
			return nil, fmt.Errorf("query block: %w", err)
		}
		results = append(results, blockResults...)
	}

	// Merge results by series
	merged := make(map[uint64]*model.QueryResult)
	for i := range results {
		hash := results[i].Series.Hash()
		if existing, ok := merged[hash]; ok {
			existing.Samples = append(existing.Samples, results[i].Samples...)
		} else {
			merged[hash] = &results[i]
		}
	}

	// Sort samples and deduplicate
	finalResults := make([]model.QueryResult, 0, len(merged))
	for _, r := range merged {
		model.Samples(r.Samples).Swap(0, 0) // Type assertion trick
		sortedSamples := model.Samples(r.Samples)
		sortedSamples = deduplicateSamples(sortedSamples)
		r.Samples = sortedSamples
		finalResults = append(finalResults, *r)
	}

	return finalResults, nil
}

// ListBlocks returns all loaded blocks.
func (e *Engine) ListBlocks() []*Block {
	e.blocksMu.RLock()
	defer e.blocksMu.RUnlock()

	blocks := make([]*Block, len(e.blocks))
	copy(blocks, e.blocks)
	return blocks
}

// Status returns the current engine status.
func (e *Engine) Status() EngineStatus {
	e.blocksMu.RLock()
	numBlocks := len(e.blocks)
	var totalSamples uint64
	for _, b := range e.blocks {
		totalSamples += b.meta.NumSamples
	}
	e.blocksMu.RUnlock()

	return EngineStatus{
		NumSeries:       e.memtable.SeriesCount(),
		NumBlocks:       numBlocks,
		TotalSamples:    totalSamples,
		MemTableSize:    e.memtable.Size(),
		MemTableMinTime: e.memtable.minTime,
		MemTableMaxTime: e.memtable.maxTime,
	}
}

// EngineStatus holds status information about the engine.
type EngineStatus struct {
	NumSeries       int
	NumBlocks       int
	TotalSamples    uint64
	MemTableSize    int64
	MemTableMinTime int64
	MemTableMaxTime int64
}

// flushLoop periodically flushes the memtable to disk.
func (e *Engine) flushLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.flushPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-e.closeCh:
			return
		case <-ticker.C:
			e.maybeFlush()
		}
	}
}

// maybeFlush flushes the memtable if it's large enough.
func (e *Engine) maybeFlush() {
	if e.memtable.Size() < e.maxMemTableSize {
		return
	}

	// Get WAL segment before flush
	walSegment := e.wal.LastSegmentID()

	// Compact memtable to block
	_, err := e.compactor.CompactMemTable(e.memtable, walSegment)
	if err != nil {
		fmt.Printf("Error compacting memtable: %v\n", err)
		return
	}

	// Clear memtable
	e.memtable.Clear()

	// Truncate old WAL segments
	if err := e.wal.Truncate(walSegment); err != nil {
		fmt.Printf("Error truncating WAL: %v\n", err)
	}
}

// compactionLoop runs periodic compaction.
func (e *Engine) compactionLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-e.closeCh:
			return
		case <-ticker.C:
			e.runCompaction()
		}
	}
}

// runCompaction runs a compaction cycle.
func (e *Engine) runCompaction() {
	plans, err := e.compactor.Plan()
	if err != nil {
		fmt.Printf("Error planning compaction: %v\n", err)
		return
	}

	for _, plan := range plans {
		_, err := e.compactor.Compact(plan)
		if err != nil {
			fmt.Printf("Error compacting: %v\n", err)
		}
	}
}

// Flush forces a flush of the memtable.
func (e *Engine) Flush() error {
	walSegment := e.wal.LastSegmentID()

	_, err := e.compactor.CompactMemTable(e.memtable, walSegment)
	if err != nil {
		return err
	}

	e.memtable.Clear()
	return e.wal.Truncate(walSegment)
}

// Close shuts down the engine.
func (e *Engine) Close() error {
	close(e.closeCh)
	e.wg.Wait()

	// Final flush
	if e.memtable.Size() > 0 {
		e.Flush()
	}

	// Close WAL
	if err := e.wal.Close(); err != nil {
		return err
	}

	// Close blocks
	e.blocksMu.Lock()
	for _, b := range e.blocks {
		b.Close()
	}
	e.blocksMu.Unlock()

	return nil
}
