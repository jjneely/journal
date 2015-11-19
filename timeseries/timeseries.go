package timeseries

import (
	"os"
)

type Journal interface {
	// Epoch returns the Unix timestamp of the first value (oldest)
	// stored in the timeseries journal.
	Epoch() int64

	// Width returns the number of bytes each value stored in the file
	// uses.  A float64 value uses 8 bytes.  The journal can only store
	// repeated values of the same type and byte width
	Width() int64

	// Interval returns the number of time units (usually seconds) between
	// each value.  If value at index 0 occurred at StartEpoch() then the
	// value at index 3 occurred at StartEpoch() + Interval() * 3
	Interval() int64

	// Read locates the first value at the given Unix timestamp in the journal
	// and will fill the provided []byte slice up to the slice length.
	// This returns the number of bytes read and any error that occurred.
	// To return a list of values starting at timestamp provide a slice
	// with length of Width() * # values.
	Read(timestamp int64, buf []byte) (int, error)

	// Write seeks to the given Unix timestamp and writes the contents
	// of the given []byte slice to the journal, extending the file length
	// on disk if needed.  Multiple values may be written by providing
	// them in the given byte slice.  They must be for sequential timestamps.
	Write(timestamp int64, buf []byte) error

	// Last returns the Unix timestamp that matches the most recent
	// value recorded in the journal.  This is the last entry in the file.
	Last() int64

	// Sync flushes data to disk.
	Sync()

	// Close closes the file.
	Close()
}

const (
	journalMagic   string = "BCKY"
	journalVersion int32  = 0
)

func Open(path string) (Journal, error) {}

func Create(path string) (Journal, error) {}

type FileJournal struct {
	epoch    int64
	width    int64
	interval int64

	fd      *os.File
	version int32
}

func (ts *BuckyTS) StartEpoch() int64 {
	return ts.epoch
}
