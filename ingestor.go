package mini_stream

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// RecordLocation is the sequence information
type RecordLocation struct {
	Filename string
	Offset   uint64
}

type Shard struct {
	ID            int
	CurrentOffset int64 // Physical byte position
	NextSeqNum    uint64
	OffsetIndex   map[uint64]RecordLocation
	mu            sync.RWMutex
	Path          string
	ActiveFile    *os.File
}

type Ingestor struct {
	MaxSegmentLength int64
	NumOfShard       int
	DataDir          string
	logger           *slog.Logger
	shards           map[int]*Shard
	// to improve the read performance. Since os.Open is costly
	fdCache map[string]*os.File
	cacheMu sync.RWMutex
	counter atomic.Uint64
}

func (i *Ingestor) SetFileSizeLimit(limit int64) {
	i.MaxSegmentLength = limit
}

func NewIngestor(numOfShards int, dataDir string, logger *slog.Logger) (*Ingestor, error) {
	i := &Ingestor{
		NumOfShard:       numOfShards,
		DataDir:          dataDir,
		logger:           logger,
		shards:           make(map[int]*Shard),
		MaxSegmentLength: 1024, // 1 kb for now
		fdCache:          make(map[string]*os.File),
	}

	for s := 0; s < numOfShards; s++ {
		shardPath := filepath.Join(dataDir, fmt.Sprintf("shard-%d", s))
		os.MkdirAll(shardPath, 0755)

		shard := &Shard{
			ID:          s,
			Path:        shardPath,
			OffsetIndex: make(map[uint64]RecordLocation),
		}

		if err := i.recoverShard(shard); err != nil {
			return nil, err // propagate instead of panic
		}

		// If no existing files found, create a fresh active file
		if shard.ActiveFile == nil {
			filename := filepath.Join(shardPath, fmt.Sprintf("%d.log", time.Now().Unix()))
			f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}
			shard.ActiveFile = f
		}

		i.shards[s] = shard
	}
	return i, nil
}

func (i *Ingestor) Ingest(key string, payload string) (uint64, error) {
	//Calculate the Shard
	shardID := i.getShard(key)

	shard := i.shards[shardID]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	data := []byte(payload)
	seqNum := shard.NextSeqNum

	// 1. Record the current byte position before writing
	byteOffset := shard.CurrentOffset

	// 2. Prepare Header: [4 bytes for data length]
	header := make([]byte, 8)

	checksum := crc32.ChecksumIEEE(data)

	binary.BigEndian.PutUint32(header, uint32(len(data)))
	binary.BigEndian.PutUint32(header[4:8], checksum)

	// Check for the size and if it crosses the segmentation length
	// rotate the file and set the active file name.

	if shard.CurrentOffset+int64(len(data))+8 > i.MaxSegmentLength {
		i.logger.Info("Rotating the current active segment file", "shard", shard, "file", shard.ActiveFile.Name())

		// During rotation, hand the old file to the read cache
		oldName := shard.ActiveFile.Name()
		i.cacheMu.Lock()
		i.fdCache[oldName] = shard.ActiveFile // keep it open, readable
		i.cacheMu.Unlock()

		newActiveFileName := filepath.Join(i.DataDir, fmt.Sprintf("shard-%d", shardID), fmt.Sprintf("%d.log", time.Now().Unix()))
		f, err := os.OpenFile(newActiveFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return seqNum, err
		}
		shard.CurrentOffset = 0
		shard.ActiveFile = f
		byteOffset = 0

	}

	// 3. Write Header + Data
	// Total written = 4 + len(data)
	n1, err := shard.ActiveFile.Write(header)
	if err != nil {
		return seqNum, err
	}

	n2, err := shard.ActiveFile.Write(data)
	if err != nil {
		return seqNum, err
	}

	// Create New Record
	record := RecordLocation{Filename: shard.ActiveFile.Name(), Offset: uint64(byteOffset)}

	// 4. Update Index and Counters
	shard.OffsetIndex[seqNum] = record
	shard.NextSeqNum++
	shard.CurrentOffset += int64(n1 + n2)

	i.logger.Info("written to disk", "shard", shardID, "offset", byteOffset)
	return seqNum, nil

}

func (i *Ingestor) Read(shardID int, seqNum uint64) (string, error) {
	shard := i.shards[shardID]
	shard.mu.RLock()
	record, exists := shard.OffsetIndex[seqNum]
	shard.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("sequence number not found")
	}

	// Use our new helper instead of os.OpenFile
	f, err := i.getFile(record.Filename)
	if err != nil {
		return "", err
	}

	// 2. Read the 8-byte header to know how much to read
	header := make([]byte, 8)
	if _, err := f.ReadAt(header, int64(record.Offset)); err != nil {
		return "", err
	}
	dataLen := binary.BigEndian.Uint32(header[:4])

	storedCRC := binary.BigEndian.Uint32(header[4:8])

	// 3. Read the actual JSON payload
	payload := make([]byte, dataLen)
	if _, err := f.ReadAt(payload, int64(record.Offset)+8); err != nil {
		return "", err
	}
	if crc32.ChecksumIEEE(payload) != storedCRC {
		return "", fmt.Errorf("checksum mismatch at offset %d: data corrupted", record.Offset)
	}

	return string(payload), nil
}

func (i *Ingestor) getShard(key string) int {
	// 1. Handle the case where no key is provided (Round Robin or Random)
	if key == "" {
		// You could use an atomic counter here to cycle through NumOfShard
		return int(i.counter.Add(1) % uint64(i.NumOfShard))
	}

	// 2. Hash the key into a 32-bit integer
	// IEEE is the standard polynomial used in Ethernet/Gzip
	hashSum := crc32.ChecksumIEEE([]byte(key))

	// 3. Use Modulo to map the hash to one of your shard slots
	// Example: If hash is 12345 and NumOfShard is 4, result is 1
	return int(hashSum % uint32(i.NumOfShard))
}

func (i *Ingestor) getFile(path string) (*os.File, error) {
	i.cacheMu.RLock()
	f, ok := i.fdCache[path]
	i.cacheMu.RUnlock()
	if ok {
		return f, nil
	}

	// Cache miss: Lock for writing
	i.cacheMu.Lock()
	defer i.cacheMu.Unlock()

	// Double-check check (in case another thread opened it while we waited for lock)
	if f, ok := i.fdCache[path]; ok {
		return f, nil
	}

	// Open the file
	newF, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	i.fdCache[path] = newF
	return newF, nil
}

func (i *Ingestor) recoverShard(shard *Shard) error {
	files, err := filepath.Glob(filepath.Join(shard.Path, "*.log"))
	if err != nil {
		return err
	}

	sort.Strings(files)
	for idx, filename := range files {
		f, err := os.OpenFile(filename, os.O_RDWR, 0644)
		if err != nil {
			return err
		}

		if idx < len(files)-1 {
			// Sealed segment — hand to cache
			i.cacheMu.Lock()
			i.fdCache[filename] = f
			i.cacheMu.Unlock()
		} else {
			shard.ActiveFile = f // last file stays as active
		}

		var offset int64
		for {
			hdr := make([]byte, 8)
			if _, err := f.ReadAt(hdr, offset); err != nil {
				break
			}
			dataLen := binary.BigEndian.Uint32(hdr[:4])
			storedCRC := binary.BigEndian.Uint32(hdr[4:8])

			// Read payload
			payload := make([]byte, dataLen)
			if _, err := f.ReadAt(payload, offset+8); err != nil {
				// Truncated write — partial record, trim it
				f.Truncate(offset)
				break
			}

			// Verify integrity
			if crc32.ChecksumIEEE(payload) != storedCRC {
				// Corrupt record — trim from here
				f.Truncate(offset)
				break
			}

			// Record is valid — index it
			shard.OffsetIndex[shard.NextSeqNum] = RecordLocation{
				Filename: filename,
				Offset:   uint64(offset),
			}

			shard.NextSeqNum++
			offset += 8 + int64(dataLen)
		}
		shard.CurrentOffset = offset

	}
	return nil

}

func (i *Ingestor) Close() {
	i.cacheMu.Lock()
	defer i.cacheMu.Unlock()

	cacheSet := make(map[*os.File]bool)

	for _, f := range i.fdCache {
		cacheSet[f] = true
		f.Close()
	}

	for _, shard := range i.shards {
		if !cacheSet[shard.ActiveFile] {
			shard.ActiveFile.Close()
		}
	}
}
