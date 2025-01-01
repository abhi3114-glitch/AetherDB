package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"aetherdb/internal/model"
)

const (
	blockVersion = 1
)

// BlockMeta contains metadata about a block.
type BlockMeta struct {
	ULID       string    `json:"ulid"`
	MinTime    int64     `json:"minTime"`
	MaxTime    int64     `json:"maxTime"`
	NumSeries  uint64    `json:"numSeries"`
	NumSamples uint64    `json:"numSamples"`
	Compaction int       `json:"compaction"` // Compaction level
	CreatedAt  time.Time `json:"createdAt"`
}

// Block represents an immutable, compressed block of time series data.
type Block struct {
	dir    string
	meta   BlockMeta
	index  *BlockIndex
	chunks *ChunkReader
}

// BlockIndex provides fast lookups for series in a block.
type BlockIndex struct {
	series map[uint64]seriesIndexEntry // series hash -> entry
}

type seriesIndexEntry struct {
	series      *model.Series
	chunkOffset int64
	chunkLength int
}

// ChunkReader reads compressed sample chunks.
type ChunkReader struct {
	file *os.File
}

// OpenBlock opens an existing block.
func OpenBlock(dir string) (*Block, error) {
	// Read meta.json
	metaPath := filepath.Join(dir, "meta.json")
	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("read meta: %w", err)
	}

	var meta BlockMeta
	if err := json.Unmarshal(metaData, &meta); err != nil {
		return nil, fmt.Errorf("parse meta: %w", err)
	}

	// Open index
	indexPath := filepath.Join(dir, "index")
	index, err := openBlockIndex(indexPath)
	if err != nil {
		return nil, fmt.Errorf("open index: %w", err)
	}

	// Open chunks
	chunksPath := filepath.Join(dir, "chunks")
	chunks, err := openChunkReader(chunksPath)
	if err != nil {
		return nil, fmt.Errorf("open chunks: %w", err)
	}

	return &Block{
		dir:    dir,
		meta:   meta,
		index:  index,
		chunks: chunks,
	}, nil
}

// Meta returns the block metadata.
func (b *Block) Meta() BlockMeta {
	return b.meta
}

// Dir returns the block directory.
func (b *Block) Dir() string {
	return b.dir
}

// Query returns samples for series matching the selector within the time range.
func (b *Block) Query(selector *model.MetricSelector, start, end int64) ([]model.QueryResult, error) {
	// Check if time range overlaps with block
	if start > b.meta.MaxTime || end < b.meta.MinTime {
		return nil, nil
	}

	var results []model.QueryResult

	for _, entry := range b.index.series {
		if !selector.Matches(entry.series) {
			continue
		}

		samples, err := b.chunks.ReadSamples(entry.chunkOffset, entry.chunkLength, start, end)
		if err != nil {
			return nil, fmt.Errorf("read samples: %w", err)
		}

		if len(samples) > 0 {
			results = append(results, model.QueryResult{
				Series:  entry.series,
				Samples: samples,
			})
		}
	}

	return results, nil
}

// Close closes the block.
func (b *Block) Close() error {
	if b.chunks != nil && b.chunks.file != nil {
		return b.chunks.file.Close()
	}
	return nil
}

// BlockWriter writes a new block.
type BlockWriter struct {
	dir        string
	meta       BlockMeta
	indexFile  *os.File
	chunksFile *os.File
	offset     int64
	entries    []seriesIndexEntry
}

// NewBlockWriter creates a new block writer.
func NewBlockWriter(dir string, minTime, maxTime int64, compactionLevel int) (*BlockWriter, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	ulid := generateULID()

	chunksPath := filepath.Join(dir, "chunks")
	chunksFile, err := os.Create(chunksPath)
	if err != nil {
		return nil, err
	}

	// Write chunks header
	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:4], 0xAE7DC001) // Magic
	header[4] = blockVersion
	if _, err := chunksFile.Write(header); err != nil {
		chunksFile.Close()
		return nil, err
	}

	return &BlockWriter{
		dir: dir,
		meta: BlockMeta{
			ULID:       ulid,
			MinTime:    minTime,
			MaxTime:    maxTime,
			Compaction: compactionLevel,
			CreatedAt:  time.Now(),
		},
		chunksFile: chunksFile,
		offset:     8,
		entries:    make([]seriesIndexEntry, 0),
	}, nil
}

// AddSeries adds a series with its samples to the block.
func (w *BlockWriter) AddSeries(series *model.Series, samples []model.Sample) error {
	if len(samples) == 0 {
		return nil
	}

	// Encode samples using Gorilla-like compression
	encoded := encodeSamples(samples)

	// Write to chunks file
	chunkOffset := w.offset
	chunkLength := len(encoded)

	// Write chunk: [4 bytes: length][data]
	lengthBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBuf, uint32(chunkLength))
	if _, err := w.chunksFile.Write(lengthBuf); err != nil {
		return err
	}
	if _, err := w.chunksFile.Write(encoded); err != nil {
		return err
	}

	w.offset += int64(4 + chunkLength)

	// Add to index
	w.entries = append(w.entries, seriesIndexEntry{
		series:      series,
		chunkOffset: chunkOffset,
		chunkLength: chunkLength,
	})

	w.meta.NumSeries++
	w.meta.NumSamples += uint64(len(samples))

	return nil
}

// Finish completes the block and writes index + meta.
func (w *BlockWriter) Finish() error {
	// Close chunks file
	if err := w.chunksFile.Sync(); err != nil {
		return err
	}
	if err := w.chunksFile.Close(); err != nil {
		return err
	}

	// Write index
	if err := w.writeIndex(); err != nil {
		return err
	}

	// Write meta.json
	metaPath := filepath.Join(w.dir, "meta.json")
	metaData, err := json.MarshalIndent(w.meta, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(metaPath, metaData, 0644); err != nil {
		return err
	}

	return nil
}

// writeIndex writes the series index.
func (w *BlockWriter) writeIndex() error {
	indexPath := filepath.Join(w.dir, "index")
	f, err := os.Create(indexPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write header
	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:4], 0xAE7D1001) // Magic
	header[4] = blockVersion
	if _, err := f.Write(header); err != nil {
		return err
	}

	// Write entry count
	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(len(w.entries)))
	if _, err := f.Write(countBuf); err != nil {
		return err
	}

	// Write each entry
	for _, entry := range w.entries {
		if err := writeIndexEntry(f, entry); err != nil {
			return err
		}
	}

	return f.Sync()
}

// Cancel aborts writing and cleans up.
func (w *BlockWriter) Cancel() error {
	if w.chunksFile != nil {
		w.chunksFile.Close()
	}
	return os.RemoveAll(w.dir)
}

// openBlockIndex opens a block index file.
func openBlockIndex(path string) (*BlockIndex, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if len(data) < 12 {
		return nil, fmt.Errorf("index too short")
	}

	// Verify header
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != 0xAE7D1001 {
		return nil, fmt.Errorf("invalid index magic: %x", magic)
	}

	entryCount := int(binary.LittleEndian.Uint32(data[8:12]))
	pos := 12

	index := &BlockIndex{
		series: make(map[uint64]seriesIndexEntry, entryCount),
	}

	for i := 0; i < entryCount; i++ {
		entry, n, err := readIndexEntry(data[pos:])
		if err != nil {
			return nil, fmt.Errorf("read entry %d: %w", i, err)
		}
		pos += n
		index.series[entry.series.Hash()] = entry
	}

	return index, nil
}

// openChunkReader opens a chunk file for reading.
func openChunkReader(path string) (*ChunkReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// Verify header
	header := make([]byte, 8)
	if _, err := f.Read(header); err != nil {
		f.Close()
		return nil, err
	}

	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != 0xAE7DC001 {
		f.Close()
		return nil, fmt.Errorf("invalid chunks magic: %x", magic)
	}

	return &ChunkReader{file: f}, nil
}

// ReadSamples reads and decodes samples from the chunk file.
func (r *ChunkReader) ReadSamples(offset int64, length int, start, end int64) ([]model.Sample, error) {
	// Seek to offset
	if _, err := r.file.Seek(offset, 0); err != nil {
		return nil, err
	}

	// Read length prefix
	lengthBuf := make([]byte, 4)
	if _, err := r.file.Read(lengthBuf); err != nil {
		return nil, err
	}

	// Read chunk data
	data := make([]byte, length)
	if _, err := r.file.Read(data); err != nil {
		return nil, err
	}

	// Decode samples
	samples := decodeSamples(data)

	// Filter by time range
	result := make([]model.Sample, 0, len(samples))
	for _, s := range samples {
		if s.Timestamp >= start && s.Timestamp <= end {
			result = append(result, s)
		}
	}

	return result, nil
}

// writeIndexEntry writes a single index entry.
func writeIndexEntry(f *os.File, entry seriesIndexEntry) error {
	// Format:
	// [8 bytes: series hash]
	// [2 bytes: name length][name]
	// [2 bytes: label count]
	// for each label: [2 bytes: key length][key][2 bytes: value length][value]
	// [8 bytes: chunk offset]
	// [4 bytes: chunk length]

	buf := make([]byte, 0, 256)

	// Hash
	hashBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(hashBuf, entry.series.Hash())
	buf = append(buf, hashBuf...)

	// Name
	nameBytes := []byte(entry.series.Name)
	lenBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(lenBuf, uint16(len(nameBytes)))
	buf = append(buf, lenBuf...)
	buf = append(buf, nameBytes...)

	// Labels
	binary.LittleEndian.PutUint16(lenBuf, uint16(len(entry.series.Labels)))
	buf = append(buf, lenBuf...)
	for k, v := range entry.series.Labels {
		keyBytes := []byte(k)
		valueBytes := []byte(v)
		binary.LittleEndian.PutUint16(lenBuf, uint16(len(keyBytes)))
		buf = append(buf, lenBuf...)
		buf = append(buf, keyBytes...)
		binary.LittleEndian.PutUint16(lenBuf, uint16(len(valueBytes)))
		buf = append(buf, lenBuf...)
		buf = append(buf, valueBytes...)
	}

	// Chunk location
	offsetBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(offsetBuf, uint64(entry.chunkOffset))
	buf = append(buf, offsetBuf...)

	lengthBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBuf, uint32(entry.chunkLength))
	buf = append(buf, lengthBuf...)

	_, err := f.Write(buf)
	return err
}

// readIndexEntry reads a single index entry.
func readIndexEntry(data []byte) (seriesIndexEntry, int, error) {
	if len(data) < 22 {
		return seriesIndexEntry{}, 0, fmt.Errorf("data too short")
	}

	pos := 0
	entry := seriesIndexEntry{}

	// Skip hash (we'll recompute from series)
	pos += 8

	// Name
	nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2
	if pos+nameLen > len(data) {
		return seriesIndexEntry{}, 0, fmt.Errorf("data too short for name")
	}
	name := string(data[pos : pos+nameLen])
	pos += nameLen

	// Labels
	if pos+2 > len(data) {
		return seriesIndexEntry{}, 0, fmt.Errorf("data too short for label count")
	}
	labelCount := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	labels := make(model.Labels)
	for i := 0; i < labelCount; i++ {
		if pos+2 > len(data) {
			return seriesIndexEntry{}, 0, fmt.Errorf("data too short for label key length")
		}
		keyLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+keyLen > len(data) {
			return seriesIndexEntry{}, 0, fmt.Errorf("data too short for label key")
		}
		key := string(data[pos : pos+keyLen])
		pos += keyLen

		if pos+2 > len(data) {
			return seriesIndexEntry{}, 0, fmt.Errorf("data too short for label value length")
		}
		valueLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+valueLen > len(data) {
			return seriesIndexEntry{}, 0, fmt.Errorf("data too short for label value")
		}
		value := string(data[pos : pos+valueLen])
		pos += valueLen

		labels[key] = value
	}

	entry.series = model.NewSeries(name, labels)

	// Chunk location
	if pos+12 > len(data) {
		return seriesIndexEntry{}, 0, fmt.Errorf("data too short for chunk location")
	}
	entry.chunkOffset = int64(binary.LittleEndian.Uint64(data[pos:]))
	pos += 8
	entry.chunkLength = int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	return entry, pos, nil
}

// encodeSamples encodes samples using delta-of-delta compression.
func encodeSamples(samples []model.Sample) []byte {
	if len(samples) == 0 {
		return nil
	}

	buf := make([]byte, 0, len(samples)*10)

	// Sample count
	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(len(samples)))
	buf = append(buf, countBuf...)

	// First sample - store full values
	tsBuf := make([]byte, 8)
	valBuf := make([]byte, 8)

	binary.LittleEndian.PutUint64(tsBuf, uint64(samples[0].Timestamp))
	binary.LittleEndian.PutUint64(valBuf, math.Float64bits(samples[0].Value))
	buf = append(buf, tsBuf...)
	buf = append(buf, valBuf...)

	// Subsequent samples - store deltas
	prevTs := samples[0].Timestamp
	prevDelta := int64(0)

	for i := 1; i < len(samples); i++ {
		// Timestamp delta-of-delta
		delta := samples[i].Timestamp - prevTs
		dod := delta - prevDelta

		// Encode delta-of-delta with variable length
		buf = appendVarint(buf, dod)

		// Value - store as full float64 for simplicity
		// (Gorilla XOR encoding would be more efficient but more complex)
		binary.LittleEndian.PutUint64(valBuf, math.Float64bits(samples[i].Value))
		buf = append(buf, valBuf...)

		prevTs = samples[i].Timestamp
		prevDelta = delta
	}

	return buf
}

// decodeSamples decodes samples from compressed format.
func decodeSamples(data []byte) []model.Sample {
	if len(data) < 4 {
		return nil
	}

	count := int(binary.LittleEndian.Uint32(data[0:4]))
	if count == 0 {
		return nil
	}

	samples := make([]model.Sample, count)
	pos := 4

	// First sample
	if pos+16 > len(data) {
		return nil
	}
	samples[0].Timestamp = int64(binary.LittleEndian.Uint64(data[pos:]))
	samples[0].Value = math.Float64frombits(binary.LittleEndian.Uint64(data[pos+8:]))
	pos += 16

	prevTs := samples[0].Timestamp
	prevDelta := int64(0)

	for i := 1; i < count; i++ {
		// Read delta-of-delta
		dod, n := readVarint(data[pos:])
		pos += n

		delta := prevDelta + dod
		samples[i].Timestamp = prevTs + delta

		// Read value
		if pos+8 > len(data) {
			break
		}
		samples[i].Value = math.Float64frombits(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8

		prevTs = samples[i].Timestamp
		prevDelta = delta
	}

	return samples
}

// appendVarint appends a variable-length signed integer.
func appendVarint(buf []byte, v int64) []byte {
	// Simple variable-length encoding
	tmp := make([]byte, 10)
	n := binary.PutVarint(tmp, v)
	return append(buf, tmp[:n]...)
}

// readVarint reads a variable-length signed integer.
func readVarint(data []byte) (int64, int) {
	v, n := binary.Varint(data)
	return v, n
}

// generateULID generates a unique ID for a block.
func generateULID() string {
	// Simple timestamp-based ID
	return fmt.Sprintf("%016x", time.Now().UnixNano())
}
