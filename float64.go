package journal

import (
	"bytes"
	"encoding/binary"
	"math"
)

// Float64ValueType implements ValueType and defines the characteristics
// of dealing with marshaling float64 values.  Float64 values are stored
// on disk with Little Endian encoding.
type Float64ValueType struct {
	null []byte
}

// NewFloat64ValueType is a constructor for a new Float64ValueType factory
// and is equivalent to new(Float64ValueType).
func NewFloat64ValueType() *Float64ValueType {
	return &Float64ValueType{}
}

// Width is always 8 bytes for Float64 values.
func (t *Float64ValueType) Width() int64 {
	return 8
}

// Null returns the 8 byte encoding of the IEEE floating point NaN.
func (t *Float64ValueType) Null() []byte {
	if t.null == nil {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, math.NaN())
		t.null = buf.Bytes()
	}

	return t.null
}

// Decode takes a byte slice presumably read from disk and decodes into
// a slice of float64 using Little Endian encoding.
func (t *Float64ValueType) Decode(buffer []byte) Values {
	floats := make([]float64, int64(len(buffer))/t.Width())
	buf := bytes.NewBuffer(buffer)
	err := binary.Read(buf, binary.LittleEndian, floats)
	if err != nil {
		return nil
	}
	return Float64Values(floats)
}

// Float64Values implements Values and wraps a float64 slice.
type Float64Values []float64

// Encode will encode (Little Endian) the float64 slice to a byte slice for
// writing to disk.
func (v Float64Values) Encode() []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, []float64(v))
	if err != nil {
		return nil
	}
	return buf.Bytes()
}
