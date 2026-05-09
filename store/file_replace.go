package store

import (
	"errors"
	"os"
)

func replaceFile(tmpPath, path string) error {
	if err := os.Rename(tmpPath, path); err == nil {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return wrapExternal(err, "remove replaced file")
	}
	return wrapExternal(os.Rename(tmpPath, path), "rename replacement file")
}
