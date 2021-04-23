package utils_test

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/nireo/kantadb/utils"
)

func TestCopyFile(t *testing.T) {
	file, err := os.Create("./test1")
	if err != nil {
		t.Fatalf("could not create test1 file: %s", err)
	}

	for i := 0; i < 1e3; i++ {
		dataToWrite := strconv.Itoa(rand.Int())
		file.Write([]byte(dataToWrite))
	}
	file.Close()

	if _, err := utils.CopyFile("./test1", "./test2"); err != nil {
		t.Fatalf("could not copy files: %s", err)
	}

	// read data from both files and compare that data
	data1, err := ioutil.ReadFile("./test1")
	if err != nil {
		t.Fatalf("could not read data from 'test1': %s", err)
	}

	data2, err := ioutil.ReadFile("./test2")
	if err != nil {
		t.Fatalf("could not read data from 'test2': %s", err)
	}

	if !reflect.DeepEqual(data1, data2) {
		t.Errorf("the files are not equal")
	}

	if err := os.Remove("./test1"); err != nil {
		t.Errorf("could not remove 'test1' file: %s", err)
	}

	if err := os.Remove("./test2"); err != nil {
		t.Errorf("could not remove 'test2' file: %s", err)
	}
}

func TestListFilesWithSuffixAndMakeDirectory(t *testing.T) {
	// create a new directory for the files testing the create directory method
	if err := utils.CreateDirectory("./test"); err != nil {
		t.Fatalf("could not create directory for files: %s", err)
	}

	// create files with different suffixes
	for i := 0; i < 20; i++ {
		filename := strconv.Itoa(rand.Int()) + ".test1"
		if _, err := os.Create(filepath.Join("./test", filename)); err != nil {
			t.Errorf("could not create file: %s", filename)
		}
	}

	for i := 0; i < 10; i++ {
		filename := strconv.Itoa(rand.Int()) + ".test2"
		if _, err := os.Create(filepath.Join("./test", filename)); err != nil {
			t.Errorf("could not create file: %s", filename)
		}
	}

	hasTest1Suffix := utils.ListFilesWithSuffix("./test", ".test1")
	hasTest2Suffix := utils.ListFilesWithSuffix("./test", ".test2")

	if len(hasTest1Suffix) != 20 {
		t.Errorf("wrong amount of .test1 files. want=20 got=%d", len(hasTest1Suffix))
	}

	if len(hasTest2Suffix) != 10 {
		t.Errorf("wrong amount of .test2 files. want=10 got=%d", len(hasTest2Suffix))
	}

	if err := os.RemoveAll("./test"); err != nil {
		t.Errorf("could not delete all files: %s", err)
	}
}
