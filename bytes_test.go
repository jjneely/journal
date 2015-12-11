package journal

import (
	"bytes"
	"testing"
)

func TestByteValues(t *testing.T) {
	data := [][]byte{[]byte("AA"),
		[]byte("BB"),
		[]byte("CC"),
		[]byte("DD")}

	values := ByteValues(data)
	raw := values.Encode()
	if !bytes.Equal(raw, []byte("AABBCCDD")) {
		t.Fatalf("Test [][]byte was encoded to %v but should be %v", raw,
			[]byte("AABBCCDD"))
	}

	factory := NewByteValueType(2, []byte("--"))
	if factory.Width() != 2 {
		t.Errorf("Bytes factory width is %d and should be %d", factory.Width(),
			2)
	}
	if !bytes.Equal(factory.Null(), []byte("--")) {
		t.Errorf("Bytes factory null value is %v but should be %v",
			factory.Null(), []byte("--"))
	}

	newData := factory.Decode(raw).(ByteValues)
	if len(newData) != 4 {
		t.Errorf("Decoded data is not the right length")
	}

	for i := range newData {
		if !bytes.Equal(newData[i], data[i]) {
			t.Errorf("Byte value corruption found")
		}
	}
}
