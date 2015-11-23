package locking

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestExclusive(t *testing.T) {
	file, err := ioutil.TempFile("/tmp", "locking_test.go")
	if err != nil {
		t.Fatal(err)
	}

	// Grab an exclusive lock
	err = Exclusive(file)
	if err != nil {
		t.Fatal(err)
	}

	file2, err := os.Open(file.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Try to lock the second fd exclusively
	err = TryExclusive(file2)
	if err == nil {
		t.Fatalf("Attempt to acquire second lock on the same file succeeded?!")
	} else {
		t.Logf("%s should be the expected error for an attempt on the second lock", err)
	}

	file2.Close()
	file.Close()

	os.Remove(file.Name())
}
