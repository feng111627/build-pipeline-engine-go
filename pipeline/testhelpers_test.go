package pipeline

import (
	"errors"
	"io/fs"
	"os"
)

var errNotExist = fs.ErrNotExist

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}

func statFile(path string) (os.FileInfo, error) {
	fi, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, errNotExist
	}
	return fi, err
}
