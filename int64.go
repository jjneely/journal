package journal

import (
	"bytes"
	"encoding/binary"
	"math"
)

// Int64ValueType implements ValueType and defines the characteristics
// of dealing with marshaling int64 values.  Int64 values are stored
// on disk with Little Endian encoding.
type Int64ValueType struct {
	null []byte
}

// NewInt64ValueType is a constructor for a new Int64ValueType factory
// and is equivalent to new(Int64ValueType).
func NewInt64ValueType() *Int64ValueType {
	return &Int64ValueType{}
}

// Width is always 8 bytes for Int64 values.
func (t *Int64ValueType) Width() int32 {
	return 8
}

// Type returns the type encoding as stored on disk
func (t *Int64ValueType) Type() int32 {
	return 0x11
}

// Null returns the 8 byte encoding of math.MinInt64 or -1 << 63
func (t *Int64ValueType) Null() []byte {
	if t.null == nil {
		// need an addressable variable to read this out of
		var null int64 = math.MinInt64
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, null)
		t.null = buf.Bytes()
	}

	return t.null
}

// Decode takes a byte slice presumably read from disk and decodes into
// a slice of int64 using Little Endian encoding.
func (t *Int64ValueType) Decode(buffer []byte) Values {
	ints := make([]int64, int32(len(buffer))/t.Width())
	buf := bytes.NewBuffer(buffer)
	err := binary.Read(buf, binary.LittleEndian, ints)
	if err != nil {
		return nil
	}
	return Int64Values(ints)
}

// Int64Values implements Values and wraps a int64 slice.
type Int64Values []int64

// Encode will encode (Little Endian) the int64 slice to a byte slice for
// writing to disk.
func (v Int64Values) Encode() []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, []int64(v))
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

// Len returns the length of the int64 slice
func (v Int64Values) Len() int {
	return len(v)
}
