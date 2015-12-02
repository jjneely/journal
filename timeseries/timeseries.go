package timeseries

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

import (
	"github.com/jjneely/journal/lock"
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

	// Meta returns the optional values stored in the header as int64
	// types.  This can be used to represent user specific metadata.
	// The on disk file format supports 3 int64s.
	Meta() []int64

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
}

// FileHeader represents the header information stored at the front of
// each FileJournal on disk representations.
type FileHeader struct {
	Magic    [4]byte  // magic number: 4 bytes
	Version  int32    // version: 4 bytes
	Width    int64    // width: 8 bytes
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

	err = lock.Share(fd)
	if err != nil {
		fd.Close()
		return nil, err
	}
	defer lock.Release(fd)

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

	return &j, nil
}

// Create attempts to create a FileJournal at the given path, creating
// any subdirectories needed by the path.  The width of the data type
// that will be stored must be given.  A float64 is 8 bytes.  The
// time units between each data point must also be given.  For a time
// series file that records data points every 60 seconds must have interval
// set to 60.  The meta parameter is a value defined by the application.
func Create(path string, width, interval int64, meta []int64) (*FileJournal, error) {
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
	defer lock.Release(fd)

	// Allocate and fill in our structs
	j := FileJournal{
		header: FileHeader{
			Magic:    Magic,
			Version:  Version,
			Width:    width,
			Interval: interval,
			Epoch:    0,
		},
		fd:       fd,
		readonly: false,
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

// Write seeks to the given Unix timestamp and writes the contents
// of the given []byte slice to the journal, extending the file length
// on disk if needed.  Multiple values may be written by providing
// them in the given byte slice.  They must be for sequential timestamps.
func (ts *FileJournal) Write(timestamp int64, values, null []byte) error {
	// Sanity Check
	if int64(len(values))%ts.header.Width != 0 {
		return fmt.Errorf("Buffer length not a multiple of width")
	}
	if int64(len(null)) != ts.header.Width {
		return fmt.Errorf("Given null value must be width bytes long")
	}
	timestamp = adjust(timestamp, ts.header.Interval)

	// Lock the file
	err := lock.Exclusive(ts.fd)
	if err != nil {
		return err
	}
	defer lock.Release(ts.fd)

	// if Epoch is 0, we need to check that's till the case
	epoch := int64(0)
	buf := make([]byte, 8)
	if ts.header.Epoch == 0 {
		_, err = ts.fd.ReadAt(buf, HeaderSize-8) // location of epoch in file
		if err != nil {
			return err
		}
		epoch = int64(binary.LittleEndian.Uint64(buf))
	}

	if epoch == 0 {
		binary.LittleEndian.PutUint64(buf, uint64(timestamp))
		_, err = ts.fd.WriteAt(buf, HeaderSize-8) // location of epoch
		if err != nil {
			return err
		}

		// update the header struct, which should now no longer change
		ts.header.Epoch = epoch
	}

	// Calculate offset
	stat, err := ts.fd.Stat()
	if err != nil {
		return err
	}
	offsetBytes := ((timestamp - ts.header.Epoch) / ts.header.Interval) * ts.header.Width

	// Write to the file
	if offsetBytes < 0 {
		// XXX: Handle this case, this is most likely a file re-write
		// and is anticipated to be a rare event
		return fmt.Errorf("Time stamp is before journal epoch")
	}
	if offsetBytes > stat.Size()-HeaderSize {
		// We need to fill in a gap of null data between the end of file
		// and where our data starts
		gapBytes := offsetBytes - stat.Size() - HeaderSize
		buf = make([]byte, gapBytes)
		for i := int64(0); i < gapBytes; i = i + ts.header.Width {
			copy(buf[i:i+ts.header.Width], null)
		}
		buf = append(buf, values...)

		_, err = ts.fd.WriteAt(buf, stat.Size())
	} else {
		// We are writing at the end of the file (normal) or somewhere in
		// the middle of the file (allowed)
		_, err = ts.fd.WriteAt(values, offsetBytes+HeaderSize)
	}
	if err != nil {
		return err
	}

	return nil
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
func (ts *FileJournal) Width() int64 {
	return ts.header.Width
}

// Interval returns the time unit interval between data values.  If the
// time series journal contains data points every 60 seconds then this
// function returns 60.
func (ts *FileJournal) Interval() int64 {
	return ts.header.Interval
}
