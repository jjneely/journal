package journal

import (
	"bytes"
)

// ByteValueType implements ValueType and defines a []byte of fixed size
// with the width and null value definable by the user.
type ByteValueType struct {
	width int32
	null  []byte
}

// NewByteValueType returns a newly allocated ByteValueType using the
// given width for the set size records, and the given null byte slice to
// represent null values.
func NewByteValueType(width int32, null []byte) *ByteValueType {
	b := new(ByteValueType)
	b.width = width
	for i := int32(len(null)); i < width; i++ {
		null = append(null, byte(0))
	}
	b.null = null[:width]
	return b
}

// Width reports the size of the []byte record and is constant for all
// records managed by this ByteValueType.
func (t *ByteValueType) Width() int32 {
	return t.width
}

// Null returns a []byte that represents null values on disk as given to
// the constructor.
func (t *ByteValueType) Null() []byte {
	return t.null
}

// Type returns the type encoding as stored on disk
func (t *ByteValueType) Type() int32 {
	if bytes.Equal(t.null, bytes.Repeat([]byte{0x0}, int(t.width))) {
		return 0x01
	}
	if bytes.HasPrefix(t.null, []byte("NULL")) {
		return 0x00
	}
	// No pre-defined null value?
	return 0x0F
}

// Decode takes a []byte slice usually read from disk to a slice of byte
// slices represented by ByteValues.
func (t *ByteValueType) Decode(buffer []byte) Values {
	b := make([][]byte, 0)
	for i := int32(0); i < int32(len(buffer)); i += t.width {
		b = append(b, buffer[i:i+t.width])
	}
	return ByteValues(b)
}

// ByteValues wraps a slice of byte slices so that they can be encoded
// to one long slice of bytes for on disk storage.
type ByteValues [][]byte

// Encode returns a byte slice representing slice of byte slices.
func (v ByteValues) Encode() []byte {
	b := make([]byte, 0)
	for i := range v {
		b = append(b, v[i]...)
	}
	return b
}

// Len returns the length of the slice of byte slices.
func (v ByteValues) Len() int {
	return len(v)
}
