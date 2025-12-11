package storage

import (
	"path/filepath"
	"testing"

	"aetherdb/internal/model"
)

func TestWAL_AppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	// Create WAL
	wal, err := NewWAL(walDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create test entries
	series := model.NewSeries("cpu_usage", model.Labels{"host": "server1"})
	samples := []model.Sample{
		{Timestamp: 1000, Value: 45.5},
		{Timestamp: 2000, Value: 50.2},
		{Timestamp: 3000, Value: 48.7},
	}

	entries := []WALEntry{
		{
			SeriesRef: 1,
			Series:    series,
			Samples:   samples,
		},
	}

	// Append entries
	if err := wal.Append(entries); err != nil {
		t.Fatalf("Failed to append entries: %v", err)
	}

	if err := wal.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Close and reopen WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	wal2, err := NewWAL(walDir)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	// Replay entries
	var replayedCount int
	err = wal2.Replay(func(entry WALEntry) error {
		replayedCount++
		if entry.SeriesRef != 1 {
			t.Errorf("Expected series ref 1, got %d", entry.SeriesRef)
		}
		if entry.Series == nil {
			t.Error("Expected series to be set")
		}
		if len(entry.Samples) != 3 {
			t.Errorf("Expected 3 samples, got %d", len(entry.Samples))
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay: %v", err)
	}

	if replayedCount != 1 {
		t.Errorf("Expected 1 replayed entry, got %d", replayedCount)
	}
}

func TestMemTable_Basic(t *testing.T) {
	mt := NewMemTable()

	// Create series
	series := model.NewSeries("memory_used", model.Labels{"host": "server1", "region": "us-east"})

	// Get or create series
	ref, created := mt.GetOrCreateSeries(series)
	if !created {
		t.Error("Expected series to be created")
	}

	// Get same series again
	ref2, created2 := mt.GetOrCreateSeries(series)
	if created2 {
		t.Error("Expected series to already exist")
	}
	if ref != ref2 {
		t.Errorf("Expected same ref, got %d and %d", ref, ref2)
	}

	// Append samples
	samples := []model.Sample{
		{Timestamp: 1000, Value: 8589934592},
		{Timestamp: 2000, Value: 8589934592},
		{Timestamp: 3000, Value: 9589934592},
	}

	if err := mt.Append(ref, samples); err != nil {
		t.Fatalf("Failed to append samples: %v", err)
	}

	// Query
	selector := model.NewMetricSelector("memory_used", nil)
	results := mt.Query(selector, 0, 5000)

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	if len(results[0].Samples) != 3 {
		t.Errorf("Expected 3 samples, got %d", len(results[0].Samples))
	}
}

func TestMemTable_TimeRangeQuery(t *testing.T) {
	mt := NewMemTable()

	series := model.NewSeries("requests", nil)
	ref, _ := mt.GetOrCreateSeries(series)

	samples := []model.Sample{
		{Timestamp: 1000, Value: 100},
		{Timestamp: 2000, Value: 200},
		{Timestamp: 3000, Value: 300},
		{Timestamp: 4000, Value: 400},
		{Timestamp: 5000, Value: 500},
	}

	mt.Append(ref, samples)

	// Query subset
	selector := model.NewMetricSelector("requests", nil)
	results := mt.Query(selector, 2000, 4000)

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	if len(results[0].Samples) != 3 {
		t.Errorf("Expected 3 samples (2000, 3000, 4000), got %d", len(results[0].Samples))
	}
}

func TestBlock_WriteAndRead(t *testing.T) {
	dir := t.TempDir()
	blockDir := filepath.Join(dir, "block_test")

	// Create block writer
	writer, err := NewBlockWriter(blockDir, 1000, 5000, 0)
	if err != nil {
		t.Fatalf("Failed to create block writer: %v", err)
	}

	// Add series with samples
	series := model.NewSeries("disk_io", model.Labels{"device": "sda"})
	samples := []model.Sample{
		{Timestamp: 1000, Value: 1024},
		{Timestamp: 2000, Value: 2048},
		{Timestamp: 3000, Value: 3072},
		{Timestamp: 4000, Value: 4096},
		{Timestamp: 5000, Value: 5120},
	}

	if err := writer.AddSeries(series, samples); err != nil {
		t.Fatalf("Failed to add series: %v", err)
	}

	if err := writer.Finish(); err != nil {
		t.Fatalf("Failed to finish block: %v", err)
	}

	// Open block
	block, err := OpenBlock(blockDir)
	if err != nil {
		t.Fatalf("Failed to open block: %v", err)
	}
	defer block.Close()

	// Query block
	selector := model.NewMetricSelector("disk_io", nil)
	results, err := block.Query(selector, 2000, 4000)
	if err != nil {
		t.Fatalf("Failed to query block: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	if len(results[0].Samples) != 3 {
		t.Errorf("Expected 3 samples, got %d", len(results[0].Samples))
	}
}

func TestEngine_WriteAndQuery(t *testing.T) {
	dir := t.TempDir()

	engine, err := NewEngine(EngineConfig{
		DataDir:         dir,
		MaxMemTableSize: 1024 * 1024, // 1MB
	})
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Write some data
	for i := 0; i < 100; i++ {
		series := model.NewSeries("http_requests", model.Labels{
			"method": "GET",
			"path":   "/api/users",
		})
		samples := []model.Sample{
			{Timestamp: int64(1000 + i*100), Value: float64(i)},
		}
		if err := engine.Write(series, samples); err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
	}

	// Query data
	selector := model.NewMetricSelector("http_requests", nil)
	results, err := engine.Query(selector, 0, 100000)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 series, got %d", len(results))
	}

	if len(results[0].Samples) != 100 {
		t.Errorf("Expected 100 samples, got %d", len(results[0].Samples))
	}
}

func TestEngine_CrashRecovery(t *testing.T) {
	dir := t.TempDir()

	// Create engine and write data
	engine, err := NewEngine(EngineConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	series := model.NewSeries("persistent_metric", nil)
	samples := []model.Sample{
		{Timestamp: 1000, Value: 42.0},
		{Timestamp: 2000, Value: 43.0},
	}
	if err := engine.Write(series, samples); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Close the engine properly (this will sync WAL)
	if err := engine.Close(); err != nil {
		t.Fatalf("Failed to close engine: %v", err)
	}

	// Reopen engine - should recover from WAL
	engine2, err := NewEngine(EngineConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("Failed to reopen engine: %v", err)
	}
	defer engine2.Close()

	// Query recovered data
	selector := model.NewMetricSelector("persistent_metric", nil)
	results, err := engine2.Query(selector, 0, 10000)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 series after recovery, got %d", len(results))
	}

	if len(results[0].Samples) != 2 {
		t.Errorf("Expected 2 samples after recovery, got %d", len(results[0].Samples))
	}
}

func TestCompression(t *testing.T) {
	samples := []model.Sample{
		{Timestamp: 1000000000, Value: 100.5},
		{Timestamp: 1000001000, Value: 101.2},
		{Timestamp: 1000002000, Value: 99.8},
		{Timestamp: 1000003000, Value: 102.3},
		{Timestamp: 1000004000, Value: 100.0},
	}

	// Encode
	encoded := encodeSamples(samples)

	// Decode
	decoded := decodeSamples(encoded)

	if len(decoded) != len(samples) {
		t.Fatalf("Expected %d samples, got %d", len(samples), len(decoded))
	}

	for i := range samples {
		if decoded[i].Timestamp != samples[i].Timestamp {
			t.Errorf("Sample %d: expected timestamp %d, got %d",
				i, samples[i].Timestamp, decoded[i].Timestamp)
		}
		if decoded[i].Value != samples[i].Value {
			t.Errorf("Sample %d: expected value %f, got %f",
				i, samples[i].Value, decoded[i].Value)
		}
	}

	// Check compression ratio
	uncompressedSize := len(samples) * 16 // 8 bytes timestamp + 8 bytes value
	compressionRatio := float64(uncompressedSize) / float64(len(encoded))
	t.Logf("Compression ratio: %.2fx (%d bytes -> %d bytes)",
		compressionRatio, uncompressedSize, len(encoded))
}
