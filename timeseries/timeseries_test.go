package timeseries

import (
	"math/rand"
	"testing"
)

// path string, width, interval int64, meta []int64
func TestFileCreate(t *testing.T) {
	meta := make([]int64, 4)
	meta[0] = rand.Int63()
	meta[1] = rand.Int63()
	meta[2] = rand.Int63()
	meta[3] = rand.Int63()
	j, err := Create("/tmp/test.jts", 8, 60, meta)
	if err != nil {
		t.Fatal(err)
	}

	j.Close()

	j, err = Open("/tmp/test.jts")
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
