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
	// each value.
	Interval() int64

	// Meta returns the value stored in the int64 or 8 byte meta field
	// in the header.  This can be used to recognize the data type stored
	// in the Journal or represent other metadata.
	Meta() int64

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
	journalMagic   string = "BKTS"
	journalVersion int32  = 0
)

type FileJournal struct {
	// File header format:  Magic identifier is 4 bytes
	version  int32 // version is the next 4 bytes
	width    int64 // width is the next 8 bytes
	interval int64 // interval is the next 8 bytes
	meta     int64 // meta is a type/metadata/maxsize field and 8 bytes
	epoch    int64 // epoch is last in the header, and 8 bytes, if epoch
	// is 0, there is no data in the file

	fd      *os.File
	datalen int64
}

// Open finds the time series journal referenced by the given path, opens
// the file and returns a FileJournal struct and any possible error.  Try to
// open the underlying file read/write.  If that fails, open the file
// read-only which means Write() calls will return an error.
func Open(path string) (*FileJournal, error) {}

// Create attempts to create a FileJournal at the given path, creating
// any subdirectories needed by the path.  The width of the data type
// that will be stored must be given.  A float64 is 8 bytes.  The
// time units between each data point must also be given.  For a time
// series file that records data points every 60 seconds must have interval
// set to 60.  The meta parameter is a value defined by the application.
func Create(path string, width, interval, meta int64) (*FileJournal, error) {

}

func (ts *FileJournal) Epoch() int64 {
	return ts.epoch
}

func (ts *FileJournal) Meta() int64 {
	return ts.meta
}

func (ts *FileJournal) Width() int64 {
	return ts.width
}

func (ts *FileJournal) Interval() int64 {
	return ts.interval
}
