// Package storage implements the storage engine for AetherDB.
package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"aetherdb/internal/model"
)

const (
	walSegmentSize = 128 * 1024 * 1024  // 128MB per segment
	walMagic       = uint32(0xAE7DB001) // Magic number for WAL
	walVersion     = uint8(1)
)

// WALEntry represents a single entry in the WAL.
type WALEntry struct {
	SeriesRef model.SeriesRef
	Series    *model.Series // Only set for new series
	Samples   []model.Sample
}

// WAL is an append-only write-ahead log for durability.
type WAL struct {
	dir        string
	mu         sync.Mutex
	segment    *os.File
	segmentID  int
	offset     int64
	maxSegSize int64
}

// NewWAL creates a new WAL in the given directory.
func NewWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create WAL dir: %w", err)
	}

	w := &WAL{
		dir:        dir,
		maxSegSize: walSegmentSize,
	}

	// Find the latest segment
	segments, err := w.listSegments()
	if err != nil {
		return nil, err
	}

	if len(segments) > 0 {
		w.segmentID = segments[len(segments)-1]
	}

	if err := w.openSegment(); err != nil {
		return nil, err
	}

	return w, nil
}

// segmentPath returns the path to a segment file.
func (w *WAL) segmentPath(id int) string {
	return filepath.Join(w.dir, fmt.Sprintf("%08d.wal", id))
}

// listSegments returns the IDs of all segment files in order.
func (w *WAL) listSegments() ([]int, error) {
	files, err := os.ReadDir(w.dir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	var segments []int
	for _, f := range files {
		var id int
		if _, err := fmt.Sscanf(f.Name(), "%08d.wal", &id); err == nil {
			segments = append(segments, id)
		}
	}
	sort.Ints(segments)
	return segments, nil
}

// openSegment opens the current segment for writing.
func (w *WAL) openSegment() error {
	path := w.segmentPath(w.segmentID)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}

	// Write header for new segments
	if info.Size() == 0 {
		header := make([]byte, 8)
		binary.LittleEndian.PutUint32(header[0:4], walMagic)
		header[4] = walVersion
		if _, err := f.Write(header); err != nil {
			f.Close()
			return err
		}
		w.offset = 8
	} else {
		w.offset = info.Size()
	}

	w.segment = f
	return nil
}

// Append writes entries to the WAL.
func (w *WAL) Append(entries []WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, entry := range entries {
		data, err := encodeWALEntry(entry)
		if err != nil {
			return fmt.Errorf("encode entry: %w", err)
		}

		// Check if we need to rotate
		if w.offset+int64(len(data)+8) > w.maxSegSize {
			if err := w.rotate(); err != nil {
				return fmt.Errorf("rotate segment: %w", err)
			}
		}

		// Write: [4 bytes length][4 bytes CRC][data]
		header := make([]byte, 8)
		binary.LittleEndian.PutUint32(header[0:4], uint32(len(data)))
		binary.LittleEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(data))

		if _, err := w.segment.Write(header); err != nil {
			return fmt.Errorf("write header: %w", err)
		}
		if _, err := w.segment.Write(data); err != nil {
			return fmt.Errorf("write data: %w", err)
		}

		w.offset += int64(len(header) + len(data))
	}

	return nil
}

// rotate creates a new segment.
func (w *WAL) rotate() error {
	if err := w.segment.Sync(); err != nil {
		return err
	}
	if err := w.segment.Close(); err != nil {
		return err
	}

	w.segmentID++
	return w.openSegment()
}

// Sync ensures all data is persisted to disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segment.Sync()
}

// Close closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.segment != nil {
		return w.segment.Close()
	}
	return nil
}

// Replay reads all entries from the WAL and calls the provided function for each.
func (w *WAL) Replay(fn func(WALEntry) error) error {
	segments, err := w.listSegments()
	if err != nil {
		return err
	}

	for _, segID := range segments {
		if err := w.replaySegment(segID, fn); err != nil {
			return fmt.Errorf("replay segment %d: %w", segID, err)
		}
	}

	return nil
}

// replaySegment replays a single segment.
func (w *WAL) replaySegment(segID int, fn func(WALEntry) error) error {
	f, err := os.Open(w.segmentPath(segID))
	if err != nil {
		return err
	}
	defer f.Close()

	// Read and verify header
	header := make([]byte, 8)
	if _, err := io.ReadFull(f, header); err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != walMagic {
		return fmt.Errorf("invalid magic number: %x", magic)
	}

	// Read entries
	for {
		entryHeader := make([]byte, 8)
		if _, err := io.ReadFull(f, entryHeader); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read entry header: %w", err)
		}

		length := binary.LittleEndian.Uint32(entryHeader[0:4])
		expectedCRC := binary.LittleEndian.Uint32(entryHeader[4:8])

		data := make([]byte, length)
		if _, err := io.ReadFull(f, data); err != nil {
			return fmt.Errorf("read entry data: %w", err)
		}

		// Verify CRC
		actualCRC := crc32.ChecksumIEEE(data)
		if actualCRC != expectedCRC {
			return fmt.Errorf("CRC mismatch: expected %x, got %x", expectedCRC, actualCRC)
		}

		entry, err := decodeWALEntry(data)
		if err != nil {
			return fmt.Errorf("decode entry: %w", err)
		}

		if err := fn(entry); err != nil {
			return err
		}
	}

	return nil
}

// Truncate removes segments before the given segment ID.
func (w *WAL) Truncate(beforeSegment int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	segments, err := w.listSegments()
	if err != nil {
		return err
	}

	for _, segID := range segments {
		if segID < beforeSegment {
			if err := os.Remove(w.segmentPath(segID)); err != nil {
				return fmt.Errorf("remove segment %d: %w", segID, err)
			}
		}
	}

	return nil
}

// LastSegmentID returns the ID of the last segment.
func (w *WAL) LastSegmentID() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segmentID
}

// encodeWALEntry encodes a WAL entry to bytes.
func encodeWALEntry(entry WALEntry) ([]byte, error) {
	// Format:
	// [8 bytes: series ref]
	// [1 byte: has series definition]
	// if has series:
	//   [2 bytes: name length][name bytes]
	//   [2 bytes: label count]
	//   for each label:
	//     [2 bytes: key length][key bytes][2 bytes: value length][value bytes]
	// [4 bytes: sample count]
	// for each sample:
	//   [8 bytes: timestamp][8 bytes: value]

	buf := make([]byte, 0, 256)

	// Series ref
	tmp := make([]byte, 8)
	binary.LittleEndian.PutUint64(tmp, uint64(entry.SeriesRef))
	buf = append(buf, tmp...)

	// Has series definition
	if entry.Series != nil {
		buf = append(buf, 1)

		// Name
		nameBytes := []byte(entry.Series.Name)
		tmp = make([]byte, 2)
		binary.LittleEndian.PutUint16(tmp, uint16(len(nameBytes)))
		buf = append(buf, tmp...)
		buf = append(buf, nameBytes...)

		// Labels
		binary.LittleEndian.PutUint16(tmp, uint16(len(entry.Series.Labels)))
		buf = append(buf, tmp...)
		for k, v := range entry.Series.Labels {
			keyBytes := []byte(k)
			valueBytes := []byte(v)
			binary.LittleEndian.PutUint16(tmp, uint16(len(keyBytes)))
			buf = append(buf, tmp...)
			buf = append(buf, keyBytes...)
			binary.LittleEndian.PutUint16(tmp, uint16(len(valueBytes)))
			buf = append(buf, tmp...)
			buf = append(buf, valueBytes...)
		}
	} else {
		buf = append(buf, 0)
	}

	// Samples
	tmp = make([]byte, 4)
	binary.LittleEndian.PutUint32(tmp, uint32(len(entry.Samples)))
	buf = append(buf, tmp...)

	tmp = make([]byte, 16)
	for _, sample := range entry.Samples {
		binary.LittleEndian.PutUint64(tmp[0:8], uint64(sample.Timestamp))
		binary.LittleEndian.PutUint64(tmp[8:16], uint64(sample.Value))
		buf = append(buf, tmp...)
	}

	return buf, nil
}

// decodeWALEntry decodes a WAL entry from bytes.
func decodeWALEntry(data []byte) (WALEntry, error) {
	if len(data) < 9 {
		return WALEntry{}, fmt.Errorf("data too short")
	}

	entry := WALEntry{}
	pos := 0

	// Series ref
	entry.SeriesRef = model.SeriesRef(binary.LittleEndian.Uint64(data[pos:]))
	pos += 8

	// Has series definition
	hasSeries := data[pos] == 1
	pos++

	if hasSeries {
		if pos+2 > len(data) {
			return WALEntry{}, fmt.Errorf("data too short for series name length")
		}
		nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2

		if pos+nameLen > len(data) {
			return WALEntry{}, fmt.Errorf("data too short for series name")
		}
		name := string(data[pos : pos+nameLen])
		pos += nameLen

		if pos+2 > len(data) {
			return WALEntry{}, fmt.Errorf("data too short for label count")
		}
		labelCount := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2

		labels := make(model.Labels)
		for i := 0; i < labelCount; i++ {
			if pos+2 > len(data) {
				return WALEntry{}, fmt.Errorf("data too short for label key length")
			}
			keyLen := int(binary.LittleEndian.Uint16(data[pos:]))
			pos += 2

			if pos+keyLen > len(data) {
				return WALEntry{}, fmt.Errorf("data too short for label key")
			}
			key := string(data[pos : pos+keyLen])
			pos += keyLen

			if pos+2 > len(data) {
				return WALEntry{}, fmt.Errorf("data too short for label value length")
			}
			valueLen := int(binary.LittleEndian.Uint16(data[pos:]))
			pos += 2

			if pos+valueLen > len(data) {
				return WALEntry{}, fmt.Errorf("data too short for label value")
			}
			value := string(data[pos : pos+valueLen])
			pos += valueLen

			labels[key] = value
		}

		entry.Series = model.NewSeries(name, labels)
	}

	// Samples
	if pos+4 > len(data) {
		return WALEntry{}, fmt.Errorf("data too short for sample count")
	}
	sampleCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	entry.Samples = make([]model.Sample, sampleCount)
	for i := 0; i < sampleCount; i++ {
		if pos+16 > len(data) {
			return WALEntry{}, fmt.Errorf("data too short for sample")
		}
		entry.Samples[i].Timestamp = int64(binary.LittleEndian.Uint64(data[pos:]))
		entry.Samples[i].Value = float64(binary.LittleEndian.Uint64(data[pos+8:]))
		pos += 16
	}

	return entry, nil
}
