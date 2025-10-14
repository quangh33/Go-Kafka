// This file implements the core storage mechanism: an append-only log file.

package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// commitLogState holds the durable state for a commit log.
type commitLogState struct {
	LastAppliedIndex     uint64           `json:"last_applied_index"`
	ProducerLastSequence map[uint64]int64 `json:"producer_last_sequence"`
}

const (
	// Each record is prefixed with an 8-byte integer indicating its length.
	lenWidth = 8
)

// CommitLog represents an append-only log file on disk.
type CommitLog struct {
	mu        sync.RWMutex
	file      *os.File
	size      int64
	state     commitLogState
	statePath string
}

// NewCommitLog creates or opens a commit log file.
func NewCommitLog(path string) (*CommitLog, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open commit log file: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	c := &CommitLog{
		file:      f,
		size:      fi.Size(),
		statePath: path + ".state",
	}

	// Load the persisted state from disk.
	if err := c.loadState(); err != nil {
		return nil, fmt.Errorf("failed to load commit log state: %w", err)
	}

	return c, nil
}

// loadState reads the .state file from disk.
func (c *CommitLog) loadState() error {
	data, err := os.ReadFile(c.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			// State file doesn't exist, start with a fresh state. This is normal.
			c.state = commitLogState{LastAppliedIndex: 0, ProducerLastSequence: make(map[uint64]int64)}
			return nil
		}
		return err
	}
	return json.Unmarshal(data, &c.state)
}

// persistState writes the current state to the .state file.
func (c *CommitLog) persistState() error {
	data, err := json.Marshal(c.state)
	if err != nil {
		return err
	}
	return os.WriteFile(c.statePath, data, 0644)
}

func (c *CommitLog) AppendIdempotent(producerID uint64, sequenceNumber int64, data []byte) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Is this a duplicate message from the producer?
	lastSeq, ok := c.state.ProducerLastSequence[producerID]
	if ok && sequenceNumber <= lastSeq {
		fmt.Println("Duplicate was skipped!")
		return -1, nil
	}

	pos := c.size

	// Write the length of the data as an 8-byte header.
	lenBuf := make([]byte, lenWidth)
	binary.BigEndian.PutUint64(lenBuf, uint64(len(data)))
	if _, err := c.file.Write(lenBuf); err != nil {
		return 0, fmt.Errorf("failed to write record length: %w", err)
	}

	// Write the actual data.
	if _, err := c.file.Write(data); err != nil {
		return 0, fmt.Errorf("failed to write record data: %w", err)
	}

	// Update the in-memory size of the log.
	c.size += int64(lenWidth + len(data))
	return pos, nil
}

// Append writes a new record to the end of the log.
func (c *CommitLog) Append(data []byte) (offset int64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pos := c.size

	// Write the length of the data as an 8-byte header.
	lenBuf := make([]byte, lenWidth)
	binary.BigEndian.PutUint64(lenBuf, uint64(len(data)))
	if _, err := c.file.Write(lenBuf); err != nil {
		return 0, fmt.Errorf("failed to write record length: %w", err)
	}

	// Write the actual data.
	if _, err := c.file.Write(data); err != nil {
		return 0, fmt.Errorf("failed to write record data: %w", err)
	}

	// Update the in-memory size of the log.
	c.size += int64(lenWidth + len(data))
	return pos, nil
}

// Read retrieves a record from a specific offset in the log.
func (c *CommitLog) Read(offset int64) ([]byte, int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if offset >= c.size {
		return nil, 0, fmt.Errorf("offset out of bounds")
	}

	// Read the length prefix.
	lenBuf := make([]byte, lenWidth)
	if _, err := c.file.ReadAt(lenBuf, offset); err != nil {
		return nil, 0, fmt.Errorf("failed to read record length: %w", err)
	}

	recordLen := binary.BigEndian.Uint64(lenBuf)

	// Read the record data.
	data := make([]byte, recordLen)
	if _, err := c.file.ReadAt(data, offset+lenWidth); err != nil {
		return nil, 0, fmt.Errorf("failed to read record data: %w", err)
	}

	nextOffset := offset + int64(lenWidth) + int64(recordLen)
	return data, nextOffset, nil
}

// Close gracefully closes the log file.
func (c *CommitLog) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.file.Close()
}

// Name returns the file name of the log.
func (c *CommitLog) Name() string {
	return c.file.Name()
}

func (c *CommitLog) GetLastAppliedIndex() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state.LastAppliedIndex
}

func (c *CommitLog) SetCommitLogState(index uint64, producerId uint64, sequenceNumber int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.LastAppliedIndex = index
	c.state.ProducerLastSequence[producerId] = sequenceNumber
	return c.persistState()
}
