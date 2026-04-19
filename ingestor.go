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

var headerPool = sync.Pool{
	New: func() interface{} {
		// Return a new slice of 8 bytes
		b := make([]byte, 8)
		return &b
	},
}

// RecordLocation is the sequence information
type RecordLocation struct {
	Filename string
	Offset   uint64
}

type Shard struct {
	ID            int
	CurrentOffset int64 // Physical byte position
	NextSeqNum    uint64
	OffsetIndex   sync.Map
	mu            sync.RWMutex
	Path          string
	ActiveFile    *os.File
	// to improve the read performance. Since os.Open is costly
	fdCache map[string]*os.File
}

type Ingestor struct {
	MaxSegmentLength int64
	NumOfShard       int
	DataDir          string
	logger           *slog.Logger
	shards           map[int]*Shard
	counter          atomic.Uint64
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
	}

	for s := 0; s < numOfShards; s++ {
		shardPath := filepath.Join(dataDir, fmt.Sprintf("shard-%d", s))
		os.MkdirAll(shardPath, 0755)

		shard := &Shard{
			ID:      s,
			Path:    shardPath,
			fdCache: make(map[string]*os.File),
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
	bufPtr := headerPool.Get().(*[]byte)
	header := *bufPtr
	defer headerPool.Put(bufPtr)

	checksum := crc32.ChecksumIEEE(data)

	binary.BigEndian.PutUint32(header, uint32(len(data)))
	binary.BigEndian.PutUint32(header[4:8], checksum)

	// Check for the size and if it crosses the segmentation length
	// rotate the file and set the active file name.

	if shard.CurrentOffset+int64(len(data))+8 > i.MaxSegmentLength {
		i.logger.Info("Rotating the current active segment file", "shard", shard, "file", shard.ActiveFile.Name())

		// During rotation, hand the old file to the read cache
		oldName := shard.ActiveFile.Name()
		shard.fdCache[oldName] = shard.ActiveFile // keep it open, readable

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
	shard.OffsetIndex.Store(seqNum, record)
	shard.NextSeqNum++
	shard.CurrentOffset += int64(n1 + n2)

	i.logger.Info("written to disk", "shard", shardID, "offset", byteOffset)
	return seqNum, nil

}

func (i *Ingestor) Read(shardID int, seqNum uint64) (string, error) {
	shard := i.shards[shardID]

	// Read — lookup:
	val, exists := shard.OffsetIndex.Load(seqNum)
	if !exists {
		return "", fmt.Errorf("sequence number not found")
	}
	record := val.(RecordLocation)

	if !exists {
		return "", fmt.Errorf("sequence number not found")
	}

	// Use our new helper instead of os.OpenFile
	f, err := i.getFile(shard, record.Filename)
	if err != nil {
		return "", err
	}

	// 2. Read the 8-byte header to know how much to read
	bufPtr := headerPool.Get().(*[]byte)
	header := *bufPtr

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
	headerPool.Put(bufPtr)
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

func (i *Ingestor) getFile(shard *Shard, path string) (*os.File, error) {
	shard.mu.RLock()
	f, ok := shard.fdCache[path]
	shard.mu.RUnlock()

	if ok {
		return f, nil
	}

	// Cache miss: Lock for writing
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Double-check check (in case another thread opened it while we waited for lock)
	if f, ok := shard.fdCache[path]; ok {
		return f, nil
	}

	// Open the file
	newF, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	shard.fdCache[path] = newF
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
			shard.fdCache[filename] = f
		} else {
			shard.ActiveFile = f // last file stays as active
		}

		var offset int64
		for {
			// 2. Read the 8-byte header to know how much to read
			bufPtr := headerPool.Get().(*[]byte)
			header := *bufPtr

			if _, err := f.ReadAt(header, offset); err != nil {
				break
			}
			dataLen := binary.BigEndian.Uint32(header[:4])
			storedCRC := binary.BigEndian.Uint32(header[4:8])

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

			// recoverShard — store:
			shard.OffsetIndex.Store(shard.NextSeqNum, RecordLocation{
				Filename: filename,
				Offset:   uint64(offset),
			})

			shard.NextSeqNum++
			offset += 8 + int64(dataLen)
			headerPool.Put(bufPtr)
		}
		shard.CurrentOffset = offset

	}
	return nil

}

func (i *Ingestor) Close() {
	for _, shard := range i.shards {
		shard.mu.Lock()
		for _, f := range shard.fdCache {
			f.Close()
		}
		shard.ActiveFile.Close()
		shard.mu.Unlock()
	}
}
