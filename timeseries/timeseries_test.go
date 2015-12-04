package timeseries

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
)

func TestFileCreateOpen(t *testing.T) {
	meta := make([]int64, 4)
	fillInt64(meta)
	j, err := Create("/tmp/test.tsj", 8, 60, meta)
	if err != nil {
		t.Fatal(err)
	}

	j.Close()

	j, err = Open("/tmp/test.tsj")
	if err != nil {
		t.Fatalf("Error opening ts journal: %s", err)
	}
	if !metaEq(j.Meta(), meta) {
		t.Errorf("Metadata does not match when re-opening journal")
	}
	if j.Width() != 8 {
		t.Errorf("Width does not match when re-opening journal")
	}
	if j.Interval() != 60 {
		t.Errorf("Interval does not match when re-opening journal")
	}
	j.Close()
}

func TestReadWrite(t *testing.T) {
	epoch := int64(1449240543)
	meta := make([]int64, 4)
	fillInt64(meta)
	j, err := Create("/tmp/test-readwrite.tsj", 8, 60, meta)
	if err != nil {
		t.Fatalf("Error creating ts journam: %s", err)
	}
	defer j.Close()

	nullValue := []byte("NULL    ") // 8 byte "null" value
	values := make([]int64, 10)
	fillInt64(values)
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, values)
	if err != nil {
		t.Fatal(err)
	}

	err = j.Write(epoch, buf.Bytes(), nullValue)
	if err != nil {
		t.Fatalf("Error writing to ts journal: %s", err)
	}
	t.Logf("Data written: %v", values)
	stat, err := j.fd.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != HeaderSize+10*j.Width() {
		t.Fatalf("Produced file does not have the right size: %d != %d",
			stat.Size(), HeaderSize+10*j.Width())
	}
}

func fillInt64(list []int64) {
	for i := 0; i < len(list); i++ {
		list[i] = rand.Int63()
	}
}

func metaEq(a, b []int64) bool {
	if a == nil && b == nil {
		return true
	}

	if a == nil || b == nil {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
