package locking

import (
	"os"
	"syscall"
)

func Exclusive(file *os.File) error {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	return nil
}

func Share(file *os.File) error {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_SH); err != nil {
		return err
	}
	return nil
}

func TryExclusive(file *os.File) error {
	lock := syscall.LOCK_EX | syscall.LOCK_NB
	if err := syscall.Flock(int(file.Fd()), lock); err != nil {
		return err
	}
	return nil
}

func TryShare(file *os.File) error {
	lock := syscall.LOCK_SH | syscall.LOCK_NB
	if err := syscall.Flock(int(file.Fd()), lock); err != nil {
		return err
	}
	return nil
}

func Release(file *os.File) error {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN); err != nil {
		return err
	}
	return nil
}
