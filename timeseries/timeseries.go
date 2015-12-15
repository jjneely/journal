package timeseries

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

import (
	. "github.com/jjneely/journal"
	"github.com/jjneely/journal/lock"
)

type Journal interface {
	// Epoch returns the Unix timestamp of the first value (oldest)
	// stored in the timeseries journal.
	Epoch() int64

	// Width returns the number of bytes each value stored in the file
	// uses.  A float64 value uses 8 bytes.  The journal can only store
	// repeated values of the same type and byte width
	Width() int32

	// Interval returns the number of time units (usually seconds) between
	// each value.
	Interval() int64

	// Meta returns the optional values stored in the header as int64
	// types.  This can be used to represent user specific metadata.
	// The on disk file format supports 3 int64s.
	Meta() []int64

	// Read locates the first value at the given Unix timestamp in the journal
	// and will fill the provided []byte slice up to the slice length.
	// This returns the number of bytes read and any error that occurred.
	// To return a list of values starting at timestamp provide a slice
	// with length of Width() * # values.
	Read(timestamp int64, n int) (Values, error)

	// Write seeks to the given Unix timestamp and writes the contents
	// of the given []byte slice to the journal, extending the file length
	// on disk if needed.  Multiple values may be written by providing
	// them in the given byte slice.  They must be for sequential timestamps.
	Write(timestamp int64, values Values) error

	// Last returns the Unix timestamp that matches the most recent
	// value recorded in the journal.  This is the last entry in the file.
	Last() int64

	// Sync flushes data to disk.
	Sync()

	// Close closes the file.
	Close()
}

const (
	Version    int32 = 0
	MaxMeta          = 4
	HeaderSize       = 64
)

var (
	Magic = [4]byte{0x42, 0x4A, 0x54, 0x53} // "BJTS"
)

// FileJournal is a struct that represents an on disk timeseries journal.
type FileJournal struct {
	header   FileHeader
	fd       *os.File
	readonly bool
	points   int64
	factory  ValueType
}

// FileHeader represents the header information stored at the front of
// each FileJournal on disk representations.
type FileHeader struct {
	Magic    [4]byte  // magic number: 4 bytes
	Version  int32    // version: 4 bytes
	Type     int32    // type code: 4 bytes
	Width    int32    // width: 4 bytes
	Interval int64    // interval: 8 bytes
	Meta     [4]int64 // meta: 4 x 8 bytes
	Epoch    int64    // epoch is last in the header, and 8 bytes

	// If epoch is 0, there is no data in the file.
	// The on disk header is 64 bytes and is designed to be constant
	// hence no length.  This is data format version 0.
}

// Open finds the time series journal referenced by the given path, opens
// the file and returns a FileJournal struct and any possible error.  Try to
// open the underlying file read/write.  If that fails, open the file
// read-only which means Write() calls will return an error.
func Open(path string) (*FileJournal, error) {
	readonly := false
	fd, err := os.OpenFile(path, os.O_RDWR, 0666)
	if os.IsPermission(err) {
		fd, err = os.Open(path)
		readonly = true
	}
	if err != nil {
		return nil, err
	}

	if readonly {
		err = lock.Share(fd)
	} else {
		err = lock.Exclusive(fd)
	}
	if err != nil {
		fd.Close()
		return nil, err
	}

	j := FileJournal{}
	j.fd = fd
	j.readonly = readonly

	err = binary.Read(j.fd, binary.LittleEndian, &(j.header))
	if err != nil {
		// We couldn't fill the header struct -- corrupt file?
		return nil, err
	}

	if j.header.Magic != Magic {
		return nil, fmt.Errorf("Not a journal timeseries: %s", path)
	}

	// Type factory
	j.factory = GetValueType(j.header.Type, j.header.Width)

	// How large are we?
	stat, err := j.fd.Stat()
	if err != nil {
		return nil, err
	}

	if (stat.Size()-HeaderSize)%int64(j.header.Width) != 0 {
		// XXX: How can we recover from a partial Write()?
		return nil, fmt.Errorf("Corrupt or partial data!")
	}

	j.points = (stat.Size() - HeaderSize) / int64(j.header.Width)
	return &j, nil
}

// Create attempts to create a FileJournal at the given path, creating
// any subdirectories needed by the path.  An implementation of ValueType
// must be given that defines the type of data to be stored.  The
// time units between each data point must also be given.  For a time
// series file that records data points every 60 seconds must have interval
// set to 60.  The meta parameter is a value defined by the application.
func Create(path string, interval int64, factory ValueType, meta []int64) (*FileJournal, error) {
	// Create the base directory, if needed
	dir := filepath.Dir(path)
	dirInfo, err := os.Stat(dir)
	if os.IsNotExist(err) {
		err2 := os.MkdirAll(dir, 0666)
		if err2 != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else if !dirInfo.IsDir() {
		return nil, fmt.Errorf("File in the way of directory creation: %s",
			dirInfo.Name())
	}

	if len(meta) > MaxMeta {
		return nil, fmt.Errorf("Length of metadata slice too long")
	}

	// Open a file handle -- truncates existing file, lock new file
	fd, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	err = lock.Exclusive(fd)
	if err != nil {
		fd.Close()
		return nil, err
	}

	// Allocate and fill in our structs
	j := FileJournal{
		header: FileHeader{
			Magic:    Magic,
			Version:  Version,
			Type:     factory.Type(),
			Width:    factory.Width(),
			Interval: interval,
			Epoch:    0,
		},
		fd:       fd,
		readonly: false,
		points:   0,
		factory:  factory,
	}
	copy(j.header.Meta[:], meta)

	// Write out the header
	err = binary.Write(j.fd, binary.LittleEndian, j.header)
	if err != nil {
		return nil, err
	}
	j.fd.Sync()

	return &j, nil
}

func adjust(timestamp, interval int64) int64 {
	return timestamp - (timestamp % interval)
}

func offset(ts *FileJournal, timestamp int64) int64 {
	timestamp = adjust(timestamp, ts.header.Interval)
	return ((timestamp - ts.header.Epoch) / ts.header.Interval) * int64(ts.header.Width)
}

// Write seeks to the given Unix timestamp and writes the contents
// of the given []byte slice to the journal, extending the file length
// on disk if needed.  Multiple values may be written by providing
// them in the given byte slice.  They must be for sequential timestamps.
func (ts *FileJournal) Write(timestamp int64, values Values) error {
	var err error
	timestamp = adjust(timestamp, ts.header.Interval)
	seekPoint := (timestamp - ts.header.Epoch) / ts.header.Interval
	addedPoints := int64(values.Len())
	buffer := make([]byte, 0)
	seek := int64(0)

	if ts.header.Epoch == 0 {
		// First write, we must write the epoch
		seek = HeaderSize - 8
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(timestamp))
		buffer = append(buffer, buf...)
	} else if seekPoint <= ts.points {
		// a "normal" write
		seek = HeaderSize + (seekPoint * int64(ts.header.Width))
		if addedPoints < ts.points-seekPoint {
			addedPoints = 0
		} else {
			addedPoints = addedPoints - (ts.points - seekPoint)
		}
	} else if seekPoint > ts.points {
		// a "gap" write
		gapPoints := seekPoint - ts.points
		for i := int64(0); i < gapPoints; i++ {
			buffer = append(buffer, ts.factory.Null()...)
		}
		addedPoints = addedPoints + gapPoints
		seek = HeaderSize + (ts.points * int64(ts.header.Width))
	} else {
		// XXX: Timestamp is before journal epoch
		return fmt.Errorf("Time stamp is before journal epoch")
	}

	// Make one Write() call
	buffer = append(buffer, values.Encode()...)
	_, err = ts.fd.WriteAt(buffer, seek) // XXX: Deal with partial writes
	if err != nil {
		return err
	}

	// Book keeping
	ts.points = ts.points + addedPoints
	if ts.header.Epoch == 0 {
		ts.header.Epoch = timestamp
	}

	return nil
}

func (ts *FileJournal) Read(timestamp int64, n int) (Values, error) {
	buf := make([]byte, int64(n)*int64(ts.header.Width))
	offsetBytes := offset(ts, timestamp) // This adjusts the timestamp
	n, err := ts.fd.ReadAt(buf, offsetBytes+HeaderSize)
	return ts.factory.Decode(buf[:n]), err
}

// Close will close the underlying file.  Future read/write operations will
// result in an error.  All file locks are released.
func (ts *FileJournal) Close() {
	ts.fd.Close()
}

// Sync will flush file contents to disk.
func (ts *FileJournal) Sync() {
	ts.fd.Sync()
}

// Epoch returns the UNIX time stamp of the first value in this time series
// journal.  A 0 value indicates the journal contains no data.
func (ts *FileJournal) Epoch() int64 {
	return ts.header.Epoch
}

// Meta returns a slice referencing the metadata optionally stored in the
// file header.
func (ts *FileJournal) Meta() []int64 {
	return ts.header.Meta[:]
}

// Width returns the width of the data values stored in the time series
// journal in bytes.  This is specified at creation time.
func (ts *FileJournal) Width() int32 {
	return ts.header.Width
}

// Interval returns the time unit interval between data values.  If the
// time series journal contains data points every 60 seconds then this
// function returns 60.
func (ts *FileJournal) Interval() int64 {
	return ts.header.Interval
}

// Last returns the most recent timestamp with a corresponding value in this
// journal.
func (ts *FileJournal) Last() int64 {
	return ts.header.Epoch + (ts.header.Interval * (ts.points - 1))
}
