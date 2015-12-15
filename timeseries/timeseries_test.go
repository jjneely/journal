package timeseries

import (
	"math"
	"math/rand"
	"testing"
)

import . "github.com/jjneely/journal"

func TestFileCreateOpen(t *testing.T) {
	meta := make([]int64, 4)
	fillInt64(meta)
	null := []byte("NULL    ")
	j, err := Create("/tmp/test.tsj", 60, NewByteValueType(8, null), meta)
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
	if j.header.Type != 0x00 {
		t.Errorf("Type encoding does not match 0x00: %x", j.header.Type)
	}
	j.Close()
}

func checkSize(t *testing.T, j *FileJournal) {
	stat, err := j.fd.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != HeaderSize+j.points*int64(j.Width()) {
		t.Errorf("Produced file does not have the right size: %d != %d",
			stat.Size(), HeaderSize+j.points*int64(j.Width()))
	}
}

func TestReadWrite(t *testing.T) {
	epoch := int64(1449240543)
	meta := make([]int64, 4)
	fillInt64(meta)
	j, err := Create("/tmp/test-readwrite.tsj", 60, NewInt64ValueType(), meta)
	if err != nil {
		t.Fatalf("Error creating ts journal: %s", err)
	}
	defer j.Close()

	values := make([]int64, 10)
	fillInt64(values)
	err = j.Write(epoch, Int64Values(values))
	if err != nil {
		t.Fatalf("Error writing to ts journal: %s", err)
	}
	t.Logf("Random values: %v", values)
	if j.header.Epoch != adjust(epoch, 60) {
		t.Errorf("Journal has the wrong epoch: %d", j.header.Epoch)
	}
	if j.points != 10 {
		t.Fatalf("Journal reports %d total points, should be 10", j.points)
	}
	checkSize(t, j)

	// 2nd write that requires a null gap
	epoch2 := epoch + (20 * 60) // 20 time units in the future
	err = j.Write(epoch2, Int64Values(values))
	if err != nil {
		t.Fatalf("Error writing to journal with gap: %s", err)
	}
	checkSize(t, j)
	if j.points != 30 {
		// There should now be 30 data points in the journal
		t.Fatalf("Journal should have 30 data points not %d", j.points)
	}

	// Re-open
	j.Close()
	j, err = Open("/tmp/test-readwrite.tsj")
	if err != nil {
		t.Fatal(err)
	}
	if j.header.Type != 0x11 {
		t.Errorf("int64 journal did not re-open with the same type: %s", j.header.Type)
	}
	if j.points != 30 {
		t.Errorf("Re-open does not see the correct number of data points: %d != %d",
			j.points, 30)
	}
	if j.header.Epoch != adjust(1449240543, 60) {
		t.Errorf("Re-open does not see the correct Epoch value: %d != %d",
			j.header.Epoch, adjust(1449240543, 60))
	}

	readData, err := j.Read(epoch, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !metaEq(values, readData.(Int64Values)) {
		t.Errorf("First 10 data points of journal do not equal test data")
	}
	readData, err = j.Read(epoch2, 10)
	if !metaEq(values, readData.(Int64Values)) {
		t.Errorf("Last 10 data points of journal do not equal test data")
	}
	readData, err = j.Read(epoch2-60, 1)
	if readData.(Int64Values)[0] != math.MinInt64 {
		t.Errorf("Int64 null values not read in correctly")
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
