package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

var counter atomic.Uint64

// RecordLocation is the sequence information
type RecordLocation struct {
	Filename string
	Offset   uint64
}

type Shard struct {
	ID            int
	CurrentOffset int64  // Physical byte position
	LastSeqNum    uint64 // Logical ID (1, 2, 3...)
	NextSeqNum    uint64
	OffsetIndex   map[uint64]RecordLocation
	mu            sync.RWMutex
	Path          string
	ActiveFile    *os.File
}

type Ingestor struct {
	MaxSegmentLength int64
	NumOfShard       int
	MaxRecords       int
	DataDir          string
	logger           *slog.Logger
	shards           map[int]*Shard
}

func NewIngestor(numOfShards int, dataDir string, logger *slog.Logger) *Ingestor {
	i := &Ingestor{
		NumOfShard:       numOfShards,
		DataDir:          dataDir,
		logger:           logger,
		MaxRecords:       1000,
		shards:           make(map[int]*Shard),
		MaxSegmentLength: 1024, // 1 kb for now
	}

	for s := 0; s < numOfShards; s++ {
		shardPath := filepath.Join(dataDir, fmt.Sprintf("shard-%d", s))
		os.MkdirAll(shardPath, 0755)

		filename := filepath.Join(shardPath, fmt.Sprintf("%d.log", time.Now().Unix()))
		f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			panic(err)
		}
		i.shards[s] = &Shard{
			ID:          s,
			Path:        shardPath,
			ActiveFile:  f,
			OffsetIndex: make(map[uint64]RecordLocation),
			NextSeqNum:  0,
		}
	}
	return i
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
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(data)))

	// Check for the size and if it crosses the segmentation length
	// rotate the file and set the active file name.

	if shard.CurrentOffset+int64(len(data))+4 > i.MaxSegmentLength {
		i.logger.Info("Rotating the current active segment file", "shard", shard, "file", shard.ActiveFile.Name())

		err := shard.ActiveFile.Close()
		if err != nil {
			return seqNum, err
		}

		newActiveFileName := filepath.Join(i.DataDir, fmt.Sprintf("shard-%d", shardID), fmt.Sprintf("%d.log", time.Now().Unix()))
		f, err := os.OpenFile(newActiveFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			panic(err)
		}
		shard.CurrentOffset = 0
		shard.ActiveFile = f

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
	defer shard.mu.RUnlock()

	record, exists := shard.OffsetIndex[seqNum]
	if !exists {
		return "", fmt.Errorf("sequence number not found")
	}

	// FIX: Open the file stored in the RecordLocation, NOT the active one
	f, err := os.OpenFile(record.Filename, os.O_RDONLY, 0644)
	if err != nil {
		return "", err
	}
	defer f.Close()

	// 1. Seek to the position
	_, err = f.Seek(int64(record.Offset), 0)
	if err != nil {
		return "", err
	}

	// 2. Read the 4-byte header to know how much to read
	header := make([]byte, 4)
	if _, err := io.ReadFull(f, header); err != nil {
		return "", err
	}
	dataLen := binary.BigEndian.Uint32(header)

	// 3. Read the actual JSON payload
	payload := make([]byte, dataLen)
	if _, err := io.ReadFull(f, payload); err != nil {
		return "", err
	}

	return string(payload), nil
}

func (i *Ingestor) getShard(key string) int {
	// 1. Handle the case where no key is provided (Round Robin or Random)
	if key == "" {
		// You could use an atomic counter here to cycle through NumOfShard
		return int(counter.Add(1) % uint64(i.NumOfShard))
	}

	// 2. Hash the key into a 32-bit integer
	// IEEE is the standard polynomial used in Ethernet/Gzip
	hashSum := crc32.ChecksumIEEE([]byte(key))

	// 3. Use Modulo to map the hash to one of your shard slots
	// Example: If hash is 12345 and NumOfShard is 4, result is 1
	return int(hashSum % uint32(i.NumOfShard))
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	stream := NewIngestor(2, "./data", logger)

	// g handles the WaitGroup and context for us
	g, _ := errgroup.WithContext(context.Background())

	// PRODUCER
	g.Go(func() error {
		for j := 0; j < 10; j++ {
			stream.Ingest("user-1", fmt.Sprintf(`{"val": %d}`, j))
			time.Sleep(100 * time.Millisecond)
		}
		return nil
	})

	// CONSUMER
	g.Go(func() error {
		var next uint64
		for next < 5 {
			val, err := stream.Read(0, next)
			if err != nil {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			fmt.Println("Consumed:", val)
			next++
		}
		return nil
	})

	// Wait() blocks until all functions in g.Go return
	if err := g.Wait(); err != nil {
		fmt.Printf("Error in stream: %v\n", err)
	}
}
