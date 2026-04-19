package mini_stream

import (
	"os"
	"sync"
)

type Shard struct {
	ID            int
	CurrentOffset int64 // Physical byte position
	NextSeqNum    uint64
	OffsetIndex   sync.Map
	mu            sync.RWMutex
	Path          string
	ActiveFile    *os.File
	// to improve the read performance. Since os.Open is costly
	fdCache     map[string]*os.File
	subMu       sync.RWMutex
	subscribers map[chan string]struct{}
}

// Helper to subscribe
func (s *Shard) Subscribe() chan string {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	if s.subscribers == nil {
		s.subscribers = make(map[chan string]struct{})
	}
	ch := make(chan string, 10) // Buffer so we don't drop messages easily
	s.subscribers[ch] = struct{}{}
	return ch
}

// Helper to unsubscribe
func (s *Shard) Unsubscribe(ch chan string) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	delete(s.subscribers, ch)
	close(ch)
}

// Helper to broadcast
func (s *Shard) Broadcast(msg string) {
	s.subMu.RLock()
	defer s.subMu.RUnlock()
	for ch := range s.subscribers {
		select {
		case ch <- msg: // Send message
		default: // Drop if channel is full (prevent blocking)
		}
	}
}
