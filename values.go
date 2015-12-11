package journal

// ValueType is an interface that defines the characteristics of a specific
// type of value and can convert a byte slice into a slice of Value.
type ValueType interface {
	// Width() returns the number of bytes needed to store 1 value.  For
	// example an int64 values would return 8.
	Width() int64

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
}
