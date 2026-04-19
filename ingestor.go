package mini_stream

import (
	"context"
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

type Topic struct {
	Name      string         // name of the topic
	Shards    map[int]*Shard // map of the shard
	NumShards int            // number of shards per topic
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
	topics           map[string]*Topic
	MaxSegmentLength int64
	DataDir          string
	logger           *slog.Logger
	counter          atomic.Uint64
	mu               sync.RWMutex
	cancelRetention  context.CancelFunc
}

func (i *Ingestor) SetFileSizeLimit(limit int64) {
	i.MaxSegmentLength = limit
}

func (i *Ingestor) CreateTopic(name string, numShards int) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if _, exists := i.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	topic := &Topic{
		Name:      name,
		NumShards: numShards,
		Shards:    make(map[int]*Shard),
	}

	for s := 0; s < numShards; s++ {
		shardPath := filepath.Join(i.DataDir, name, fmt.Sprintf("shard-%d", s))
		os.MkdirAll(shardPath, 0755)

		shard := &Shard{
			ID:      s,
			Path:    shardPath,
			fdCache: make(map[string]*os.File),
		}

		if err := i.recoverShard(shard); err != nil {
			return err
		}

		if shard.ActiveFile == nil {
			filename := filepath.Join(shardPath, fmt.Sprintf("%d.log", time.Now().Unix()))
			f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				return err
			}
			shard.ActiveFile = f
		}
		topic.Shards[s] = shard
	}

	i.topics[name] = topic
	return nil
}

func (i *Ingestor) RecoverTopics() error {
	// Scan dataDir for topic directories
	entries, err := os.ReadDir(i.DataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // fresh start, no topics yet
		}
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		topicName := entry.Name()

		// Count shards by scanning shard-N directories
		shardEntries, err := os.ReadDir(filepath.Join(i.DataDir, topicName))
		if err != nil {
			continue
		}

		numShards := 0
		for _, se := range shardEntries {
			if se.IsDir() {
				numShards++
			}
		}
		if numShards == 0 {
			continue
		}

		i.logger.Info("recovering topic", "topic", topicName, "shards", numShards)
		if err := i.CreateTopic(topicName, numShards); err != nil {
			// Already exists is fine
			i.logger.Info("topic already exists", "topic", topicName)
		}
	}
	return nil
}

func NewIngestor(dataDir string, logger *slog.Logger) (*Ingestor, error) {
	cwd, _ := os.Getwd()
	logger.Info("Starting Ingestor", "working_dir", cwd, "data_dir", dataDir)

	i := &Ingestor{
		DataDir:          dataDir,
		logger:           logger,
		MaxSegmentLength: 1024, // 1 kb for now
		topics:           make(map[string]*Topic),
	}

	// Recover existing topics from disk
	if err := i.RecoverTopics(); err != nil {
		return nil, err
	}

	// Start background retention after all shards are ready
	i.startRetention(24 * time.Hour) // keep last 24 hours

	return i, nil
}

func (i *Ingestor) Ingest(topicName, key, payload string) (uint64, int, error) {

	i.mu.RLock()
	topic, exists := i.topics[topicName]
	i.mu.RUnlock()
	if !exists {
		return 0, 0, fmt.Errorf("topic %s not found", topicName)
	}

	//Calculate the Shard
	shardID := i.getShard(key, topic.NumShards)

	shard := topic.Shards[shardID]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	data := []byte(payload)
	seqNum := shard.NextSeqNum

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

		newActiveFileName := filepath.Join(i.DataDir, topicName, fmt.Sprintf("shard-%d", shardID), fmt.Sprintf("%d.log", time.Now().Unix()))

		f, err := os.OpenFile(newActiveFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return seqNum, shardID, err
		}
		shard.CurrentOffset = 0
		shard.ActiveFile = f

	}
	// 1. Record the current byte position before writing
	byteOffset := shard.CurrentOffset

	// 3. Write Header + Data
	// Total written = 4 + len(data)
	n1, err := shard.ActiveFile.Write(header)
	if err != nil {
		return seqNum, shardID, err
	}

	n2, err := shard.ActiveFile.Write(data)
	if err != nil {
		return seqNum, shardID, err
	}

	// Create New Record
	record := RecordLocation{Filename: shard.ActiveFile.Name(), Offset: uint64(byteOffset)}

	// 4. Update Index and Counters
	shard.OffsetIndex.Store(seqNum, record)
	shard.NextSeqNum++
	shard.CurrentOffset += int64(n1 + n2)

	i.logger.Info("written to disk", "shard", shardID, "offset", byteOffset)
	return seqNum, shardID, nil

}

func (i *Ingestor) Read(topicName string, shardID int, seqNum uint64) (string, error) {
	i.mu.RLock()
	topic, exists := i.topics[topicName]
	i.mu.RUnlock()
	if !exists {
		return "", fmt.Errorf("topic %s not found", topicName)
	}

	shard := topic.Shards[shardID]

	// DEBUG: Dump the index state
	count := 0
	shard.OffsetIndex.Range(func(k, v any) bool {
		count++
		return true
	})

	// Read — lookup:
	val, exists := shard.OffsetIndex.Load(seqNum)
	if !exists {
		return "", fmt.Errorf("sequence number not found")
	}
	record := val.(RecordLocation)

	// Use our new helper instead of os.OpenFile
	f, err := i.getFile(shard, record.Filename)
	if err != nil {
		return "", err
	}

	// 2. Read the 8-byte header to know how much to read
	bufPtr := headerPool.Get().(*[]byte)
	header := *bufPtr
	defer headerPool.Put(bufPtr)

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

func (i *Ingestor) getShard(key string, shardNum int) int {
	// 1. Handle the case where no key is provided (Round Robin or Random)
	if key == "" {
		// You could use an atomic counter here to cycle through NumOfShard
		return int(i.counter.Add(1) % uint64(shardNum))
	}

	// 2. Hash the key into a 32-bit integer
	// IEEE is the standard polynomial used in Ethernet/Gzip
	hashSum := crc32.ChecksumIEEE([]byte(key))

	// 3. Use Modulo to map the hash to one of your shard slots
	// Example: If hash is 12345 and NumOfShard is 4, result is 1
	return int(hashSum % uint32(shardNum))
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
			hdr := make([]byte, 8) // plain alloc, no pool needed at startup
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

			// recoverShard — store:
			shard.OffsetIndex.Store(shard.NextSeqNum, RecordLocation{
				Filename: filename,
				Offset:   uint64(offset),
			})

			shard.NextSeqNum++
			offset += 8 + int64(dataLen)
		}
		shard.CurrentOffset = offset

	}
	return nil

}

func (i *Ingestor) Close() {
	if i.cancelRetention != nil {
		i.cancelRetention()
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, topic := range i.topics {
		for _, shard := range topic.Shards {
			shard.mu.Lock()
			for _, f := range shard.fdCache {
				f.Close()
			}
			shard.ActiveFile.Close()
			shard.mu.Unlock()
		}
	}
}

func (i *Ingestor) startRetention(ttl time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	i.cancelRetention = cancel
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				i.runRetention(ttl)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (i *Ingestor) runRetention(ttl time.Duration) {
	cutoff := time.Now().Add(-ttl).Unix()
	i.mu.RLock()
	defer i.mu.RUnlock()
	for _, topic := range i.topics {
		for _, shard := range topic.Shards {
			shard.mu.Lock()

			files, _ := filepath.Glob(filepath.Join(shard.Path, "*.log"))
			sort.Strings(files)

			for _, filename := range files {
				// Never delete the active file
				if filename == shard.ActiveFile.Name() {
					continue
				}

				// Parse timestamp from filename
				base := filepath.Base(filename)
				var ts int64
				fmt.Sscanf(base, "%d.log", &ts)

				if ts < cutoff {
					// Evict from fd cache
					if f, ok := shard.fdCache[filename]; ok {
						f.Close()
						delete(shard.fdCache, filename)
					}
					// 2. Evict index entries pointing to this file
					shard.OffsetIndex.Range(func(key, value any) bool {
						loc := value.(RecordLocation)
						if loc.Filename == filename {
							shard.OffsetIndex.Delete(key)
						}
						return true
					})
					os.Remove(filename)
					i.logger.Info("compacted segment", "file", filename)
				}
			}
			shard.mu.Unlock()
		}
	}

}
