package utils

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

// CreateDirectory creates a directory named by the parameter `dir`
func CreateDirectory(dir string) error {
	if err := os.Mkdir(dir, 0755); err != nil {
		return err
	}

	return nil
}

// ListFilesWithSuffix gives a list of filenames with a given suffix. This is used
// for parsing log and sstable files. It also creates the directory if it cannot parse it.
func ListFilesWithSuffix(dir, suffix string) []string {
	paths, err := ioutil.ReadDir(dir)
	if err != nil {
		CreateDirectory(dir)
		return []string{}
	}

	var pathStrings []string
	for _, path := range paths {
		if !strings.HasSuffix(path.Name(), suffix) {
			continue
		}

		filePath := filepath.Join(dir, path.Name())
		pathStrings = append(pathStrings, filePath)
	}

	return pathStrings
}
