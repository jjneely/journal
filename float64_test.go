package journal

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func TestFloat64Values(t *testing.T) {
	data := []float64{3.14159, 6.28, 2.71828, 1.61803}

	values := Float64Values(data)
	raw := values.Encode()

	// my decode
	buf := new(bytes.Buffer)
	null := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, data)
	_ = binary.Write(null, binary.LittleEndian, math.NaN())

	if !bytes.Equal(raw, buf.Bytes()) {
		t.Fatalf("Encode to bytes did not produce the correct []byte slice")
	}

	factory := NewFloat64ValueType()
	if factory.Width() != 8 {
		t.Errorf("Bytes factory width is %d and should be %d", factory.Width(),
			8)
	}
	if !bytes.Equal(factory.Null(), null.Bytes()) {
		t.Errorf("Bytes factory null value is %v but should be %v",
			factory.Null(), []byte("--"))
	}

	newData := factory.Decode(raw).(Float64Values)
	if len(newData) != 4 {
		t.Errorf("Decoded data is not the right length %d instead of 4",
			len(newData))
	}

	for i := range newData {
		if newData[i] != data[i] {
			t.Errorf("Float64 encode/decode corruption found")
		}
	}
}
