package journal

import (
	"bytes"
)

// ValueType is an interface that defines the characteristics of a specific
// type of value and can convert a byte slice into a slice of Value.
type ValueType interface {
	// Type returns a unique int32 that identifies this ValueType as stored
	// on disk.  This must also
	Type() int32

	// Width() returns the number of bytes needed to store 1 value.  For
	// example an int64 values would return 8.
	Width() int32

	// Null returns a byte slice that must be Width() bytes long that
	// represents how to store a null value on disk.  Float64 implementations
	// might use the NaN value.
	Null() []byte

	// Decode takes a byte slice read from disk which is a multiple of
	// Width() bytes and returns a Values interface representing a slice
	// of values of the encoded data type.
	Decode(buffer []byte) Values
}

// Values is an interface that represents an underlying slice of some
// specific data type.  Perhaps int64, or float64.  This is responsible
// for encoding the slice into []byte to represent how the data will be
// stored on disk.  It is assumed that this is based on a slice type and
// that a type assertion can be use to transmute this interface into
// a native slice.
type Values interface {
	// Encode returns a byte slice representing the values of the underlying
	// slice as it will be stored on disk.  Each encoded value must use
	// a fixed width as defined by the matching ValueType struct.
	Encode() []byte

	// Len returns the length of the underlying slice.
	Len() int
}

// GetValueType takes an integer encoding of a type and width as stored on
// disk and returns the correct ValueType implementation.
func GetValueType(t, w int32) ValueType {
	// If you add ValueType instances, or different incantations of the
	// ByteValueType you'll need to update this function.  Make sure your
	// ValueType implementation returns the correct type.
	switch t {
	case 0x00, 0x0F:
		// This is mostly for testing
		// 0x0F is an unknown null value
		null := []byte("NULL")
		if w > 4 {
			null = append(null, bytes.Repeat([]byte(" "), int(w-4))...)
		}
		return NewByteValueType(w, null[0:w])
	case 0x01:
		// byte records with null == 0x0
		return NewByteValueType(w, bytes.Repeat([]byte{0x0}, int(w)))
	case 0x10:
		// Your standard 8 byte wide float64 records
		return NewFloat64ValueType()
	case 0x11:
		// int64 8 byte wide implementation, Null = MinInt64
		return NewInt64ValueType()
	}

	// We should not be here
	panic("Unimplemented journal data type")
	return nil
}
