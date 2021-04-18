package kantadb_test

import (
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nireo/kantadb"
)

func TestFolderCreated(t *testing.T) {
	db := kantadb.New(kantadb.DefaultConfiguration())
	// start running the database services
	db.Run()

	if _, err := os.Stat(db.GetDirectory()); os.IsNotExist(err) {
		t.Errorf("could not create new directory")
	}

	// remove the newly generated folder
	if err := os.Remove(db.GetDirectory()); err != nil {
		log.Printf("could not delete database folder")
	}
}

func TestBasicMemoryOperations(t *testing.T) {
	db := kantadb.New(kantadb.DefaultConfiguration())
	db.Run()

	// test placing values into the database
	keys := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	for _, key := range keys {
		db.Put(key, "value-"+key)
	}

	// test getting values
	for _, key := range keys {
		if _, ok := db.Get(key); !ok {
			t.Errorf("could not find key %q in the database", key)
		}
	}

	// test updating
	db.Put("1", "newvalue")

	val, ok := db.Get("1")
	if !ok {
		t.Errorf("error getting value from database")
	}

	if val != "newvalue" {
		t.Errorf("value wasn't updated. want=%q, got=%q", "newvalue", val)
	}

	// remove the newly generated folder
	if err := os.Remove(db.GetDirectory()); err != nil {
		log.Printf("could not delete database folder")
	}
}

func TestAllGets(t *testing.T) {
	db := kantadb.New(kantadb.DefaultConfiguration())
	db.Run()

	// since the max value is 128
	rand.Seed(time.Now().UnixNano())

	stored := []string{}
	for i := 0; i < 10000; i++ {
		randomNumber := rand.Int()
		// store some random numbers and make sure we can find them
		if randomNumber%5 == 0 {
			stored = append(stored, strconv.Itoa(randomNumber))
		}

		// write this value into the database
		db.Put(strconv.Itoa(randomNumber), "value-"+strconv.Itoa(randomNumber))
	}

	// regardless of which process the key-value pair is in i.e. in-memory, queue or sstable,
	// we should be able to find the value
	for _, key := range stored {
		if _, ok := db.Get(key); !ok {
			t.Errorf("error getting key: %s", key)
		}
	}

	// remove the newly generated folder
	if err := os.RemoveAll(db.GetDirectory()); err != nil {
		log.Printf("could not delete database folder")
	}
}

func TestSSTableCreation(t *testing.T) {
	db := kantadb.New(kantadb.DefaultConfiguration())
	db.Run()

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 4000; i++ {
		randomNumber := rand.Int()
		db.Put(strconv.Itoa(randomNumber), "value-"+strconv.Itoa(randomNumber))
	}

	// wait for all of the sstables to go through to disk
	time.Sleep(time.Millisecond * 200)

	sstables, err := ioutil.ReadDir(db.GetDirectory())
	if err != nil {
		t.Errorf("could not find files in the testfolder: %s", err)
	}

	// since the maximum size of the memory table is 1028 so there should be 3 sstables created.
	if len(sstables) == 0 {
		t.Errorf("there were no files in the directory")
	}

	if err := os.RemoveAll(db.GetDirectory()); err != nil {
		t.Errorf("could not delete database folder")
	}
}

func TestPersistance(t *testing.T) {
	db1 := kantadb.New(kantadb.DefaultConfiguration())
	db1.Run()

	rand.Seed(time.Now().UnixNano())

	stored := []string{}
	for i := 0; i < 2000; i++ {
		randomNumber := rand.Int()
		// store some random numbers and make sure we can find them
		if randomNumber%7 == 0 {
			stored = append(stored, strconv.Itoa(randomNumber))
		}

		// write this value into the database
		db1.Put(strconv.Itoa(randomNumber), "value-"+strconv.Itoa(randomNumber))
	}

	if err := db1.Stop(); err != nil {
		t.Errorf("could not stop running the database")
	}

	// wait for all of the data to be written
	time.Sleep(time.Second)

	db2 := kantadb.New(kantadb.DefaultConfiguration())
	db2.Run()

	for _, key := range stored {
		if _, ok := db2.Get(key); !ok {
			t.Errorf("error getting key: %s", key)
		}
	}

	time.Sleep(time.Millisecond * 100)
	if err := os.RemoveAll(db1.GetDirectory()); err != nil {
		t.Errorf("could not delete database folder: %s", err)
	}
}

func TestDelete(t *testing.T) {
	db := kantadb.New(kantadb.DefaultConfiguration())
	db.Run()

	// create keys
	stored := []string{}
	for i := 0; i < 2000; i++ {
		randomNumber := rand.Int()
		if randomNumber%7 == 0 {
			stored = append(stored, strconv.Itoa(randomNumber))
		}
		db.Put(strconv.Itoa(randomNumber), "value-"+strconv.Itoa(randomNumber))
	}

	// delete them
	for _, key := range stored {
		db.Delete(key)
	}

	// check that they cannot be found
	for _, key := range stored {
		if _, ok := db.Get(key); ok {
			t.Errorf("got key even though deleted: %s", key)
		}
	}

	if err := os.RemoveAll(db.GetDirectory()); err != nil {
		t.Errorf("error removing database folder: %s", err)
	}
}

func TestLogFileCreation(t *testing.T) {
	db := kantadb.New(kantadb.DefaultConfiguration())
	db.Run()

	db.Put("test1", "value2")
	db.Put("test2", "value2")
	db.Put("test3", "value2")
	db.Put("test4", "value2")
	db.Put("test5", "value2")
	db.Put("test6", "value2")
	db.Put("test7", "value2")

	files, err := ioutil.ReadDir(db.GetDirectory())
	if err != nil {
		t.Errorf("could not find files in the testfolder: %s", err)
	}

	if len(files) != 1 {
		t.Fatalf("there was a wrong number of log files. got=%d", len(files))
	}

	if !strings.HasSuffix(files[0].Name(), ".lg") {
		t.Errorf("the file is not a log file got filename: %s", files[0].Name())
	}

	if err := os.RemoveAll(db.GetDirectory()); err != nil {
		t.Errorf("error removing database folder: %s", err)
	}
}

func TestFilterFileCreation(t *testing.T) {
	db := kantadb.New(kantadb.DefaultConfiguration())
	db.Run()

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 1e5; i++ {
		randomNumber := rand.Int()
		db.Put(strconv.Itoa(randomNumber), "value-"+strconv.Itoa(randomNumber))
	}

	// wait for all of the sstables to go through to disk
	time.Sleep(time.Millisecond * 200)

	sstables, err := ioutil.ReadDir(db.GetDirectory())
	if err != nil {
		t.Errorf("could not find files in the testfolder: %s", err)
	}

	if len(sstables) == 0 {
		t.Errorf("there were no files in the directory")
	}

	// go through all of the sstables and make sure they have a bloom filter file.
	for _, ssFile := range sstables {
		if strings.HasSuffix(ssFile.Name(), ".ss") {
			withoutSuffix := strings.TrimSuffix(ssFile.Name(), ".ss")
			if _, err := os.Stat(filepath.Join(db.GetDirectory(), withoutSuffix+".fltr")); os.IsNotExist(err) {
				t.Errorf("a filter file does not exist for %s", ssFile.Name())
			}
		}
	}

	if err := os.RemoveAll(db.GetDirectory()); err != nil {
		t.Errorf("could not delete database folder")
	}
}

func TestEverySSTableHasFilter(t *testing.T) {
	db := kantadb.New(kantadb.DefaultConfiguration())
	db.Run()

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 1e5; i++ {
		randomNumber := rand.Int()
		db.Put(strconv.Itoa(randomNumber), "value-"+strconv.Itoa(randomNumber))
	}

	// wait for all of the sstables to go through to disk
	time.Sleep(time.Millisecond * 200)

	sstables, err := ioutil.ReadDir(db.GetDirectory())
	if err != nil {
		t.Errorf("could not find files in the testfolder: %s", err)
	}

	if len(sstables) == 0 {
		t.Errorf("there were no files in the directory")
	}

	// go through all of the sstables and make sure they have a bloom filter file.
	ssFileCount := 0
	fltrFileCount := 0

	for _, file := range sstables {
		if strings.HasSuffix(file.Name(), ".ss") {
			ssFileCount++
		}

		if strings.HasSuffix(file.Name(), ".fltr") {
			fltrFileCount++
		}
	}

	if ssFileCount != fltrFileCount {
		t.Errorf("there weren't the same number of .ss and .fltr files")
	}

	if err := os.RemoveAll(db.GetDirectory()); err != nil {
		t.Errorf("could not delete database folder")
	}
}

func TestFullCompaction(t *testing.T) {
	db := kantadb.New(kantadb.DefaultConfiguration())
	db.Run()

	rand.Seed(time.Now().UnixNano())

	keys := []string{}
	for i := 0; i < 8000; i++ {
		randomNumber := rand.Int()

		key := strconv.Itoa(randomNumber)
		db.Put(key, "value-"+strconv.Itoa(randomNumber))

		if randomNumber%7 == 0 {
			keys = append(keys, key)
		}
	}

	// wait for all of the sstables to go through to disk
	time.Sleep(time.Millisecond * 200)

	sstables, err := ioutil.ReadDir(db.GetDirectory())
	if err != nil {
		t.Errorf("could not find files in the testfolder: %s", err)
	}

	if len(sstables) == 0 {
		t.Errorf("there were no files in the directory")
	}

	// go through all of the sstables and make sure they have a bloom filter file.
	ssFileCount := 0

	for _, file := range sstables {
		if strings.HasSuffix(file.Name(), ".ss") {
			ssFileCount++
		}
	}

	// we want to create a single sstable which contains all of the other sstables.
	// so that:
	if err := db.CompactNTables(ssFileCount); err != nil {
		t.Errorf("error while compacting files: %s", err)
	}

	// go through all of the sstables and make sure they have a bloom filter file.
	ssFileCount = 0
	fltrCount := 0
	for _, file := range sstables {
		if strings.HasSuffix(file.Name(), ".ss") {
			ssFileCount++
		}

		if strings.HasSuffix(file.Name(), ".fltr") {
			fltrCount++
		}
	}

	if ssFileCount != 1 {
		t.Errorf("there are more than one sstables after compaction")
	}

	if fltrCount != 1 {
		t.Errorf("there are more than one filter file after completion")
	}

	for _, key := range keys {
		if _, ok := db.Get(key); !ok {
			t.Errorf("could not get key: %s", key)
		}
	}

	if err := os.RemoveAll(db.GetDirectory()); err != nil {
		t.Errorf("could not delete database folder")
	}
}

func TestPartialCompaction(t *testing.T) {
	db := kantadb.New(kantadb.DefaultConfiguration())
	db.Run()

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 20000; i++ {
		randomNumber := rand.Int()

		key := strconv.Itoa(randomNumber)
		db.Put(key, "value-"+strconv.Itoa(randomNumber))
	}

	// wait for all of the sstables to go through to disk
	time.Sleep(time.Millisecond * 200)

	sstables, err := ioutil.ReadDir(db.GetDirectory())
	if err != nil {
		t.Errorf("could not find files in the testfolder: %s", err)
	}

	if len(sstables) == 0 {
		t.Errorf("there were no files in the directory")
	}

	// go through all of the sstables and make sure they have a bloom filter file.
	ssFileCount := 0

	for _, file := range sstables {
		if strings.HasSuffix(file.Name(), ".ss") {
			ssFileCount++
		}
	}

	if err := db.CompactNTables(2); err != nil {
		t.Errorf("error while compacting files: %s", err)
	}

	time.Sleep(200 * time.Millisecond)

	// count the tables to see if there is a right amount.
	newFileCount := 0
	fltrCount := 0
	for _, file := range sstables {
		if strings.HasSuffix(file.Name(), ".ss") {
			newFileCount++
		}

		if strings.HasSuffix(file.Name(), ".fltr") {
			fltrCount++
		}
	}

	// we use -1 since the compaction function deletes 2 sstables but then creates an additional
	// sstable that contains all of the data.
	if db.GetTableSize() != ssFileCount-1 {
		t.Errorf("wrong amount of sstables. got=%d want=%d", newFileCount, ssFileCount-1)
	}

	if db.GetTableSize() != ssFileCount-1 {
		t.Errorf("wrong amount of filters. got=%d want=%d", fltrCount, ssFileCount-1)
	}

	if err := os.RemoveAll(db.GetDirectory()); err != nil {
		t.Errorf("could not delete database folder")
	}
}
