package utils

import (
	"fmt"
	"io"
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

func CopyFile(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer destination.Close()
	buf := make([]byte, 1<<16)
	for {
		n, err := source.Read(buf)
		if err != nil && err != io.EOF {
			PrintDebug("error reading original file into buffer")
		}

		if n == 0 {
			break
		}

		if _, err := destination.Write(buf[:n]); err != nil {
			PrintDebug("could not write %d bytes", err)
		}
	}
	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}
