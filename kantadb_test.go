package kantadb_test

import (
	"fmt"
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

// create a database instance and remove the database stuff after running.
func createTestDatabase(t *testing.T) *kantadb.DB {
	t.Helper()
	db := kantadb.New(kantadb.DefaultConfiguration())
	// start running the database services
	db.Run()

	t.Cleanup(func() {
		// remove all the persistance related data
		if err := os.RemoveAll(db.GetDirectory()); err != nil {
			log.Printf("could not delete database folder")
		}
	})

	return db
}

func writeNValues(db *kantadb.DB, N int) {
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 10000; i++ {
		randomNumber := rand.Int()
		db.Put(strconv.Itoa(randomNumber), "value-"+strconv.Itoa(randomNumber))
	}
}

func writeNAndStore(db *kantadb.DB, N int) []string {
	rand.Seed(time.Now().UnixNano())

	stored := []string{}
	for i := 0; i < 10000; i++ {
		randomNumber := rand.Int()

		asString := strconv.Itoa(randomNumber)

		stored = append(stored, asString)
		db.Put(asString, "value-"+strconv.Itoa(randomNumber))
	}

	return stored
}

func TestFolderCreated(t *testing.T) {
	db := createTestDatabase(t)

	if _, err := os.Stat(db.GetDirectory()); os.IsNotExist(err) {
		t.Errorf("could not create new directory")
	}
}

func TestBasicMemoryOperations(t *testing.T) {
	db := createTestDatabase(t)
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
}

func TestAllGets(t *testing.T) {
	db := createTestDatabase(t)

	// since the max value is 128
	rand.Seed(time.Now().UnixNano())
	stored := writeNAndStore(db, 1000)

	// regardless of which process the key-value pair is in i.e. in-memory, queue or sstable,
	// we should be able to find the value
	for _, key := range stored {
		if _, ok := db.Get(key); !ok {
			t.Errorf("error getting key: %s", key)
		}
	}
}

func TestSSTableCreation(t *testing.T) {
	db := createTestDatabase(t)
	writeNValues(db, 4000)

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
}

func TestPersistance(t *testing.T) {
	db1 := createTestDatabase(t)
	stored := writeNAndStore(db1, 2000)

	if err := db1.Stop(); err != nil {
		t.Errorf("could not stop running the database")
	}

	// wait for all of the data to be written
	time.Sleep(time.Second)

	// don't create the database using the test helper since the cleanup functions will overlap
	// note that since the database folder is the same the test cleanup will properly clean the data.
	db2 := kantadb.New(kantadb.DefaultConfiguration())
	db2.Run()

	for _, key := range stored {
		if _, ok := db2.Get(key); !ok {
			t.Errorf("error getting key: %s", key)
		}
	}

	time.Sleep(time.Millisecond * 100)
}

func TestDelete(t *testing.T) {
	db := createTestDatabase(t)
	stored := writeNAndStore(db, 2000)

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
}

func TestLogFileCreation(t *testing.T) {
	db := createTestDatabase(t)

	db.Put("test1", "value2")
	db.Put("test2", "value2")
	db.Put("test3", "value2")
	db.Put("test4", "value2")
	db.Put("test5", "value2")
	db.Put("test6", "value2")
	db.Put("test7", "value2")

	// there should just be one log file created
	files, err := ioutil.ReadDir(db.GetDirectory())
	if err != nil {
		t.Errorf("could not get files from directory")
	}

	// count the log files
	lgCount := 0
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".lg") {
			lgCount++
		}
	}

	if lgCount != 1 {
		t.Errorf("should only get a single log file, got=%d", lgCount)
	}
}

func TestLogFileRecovery(t *testing.T) {
	db := createTestDatabase(t)

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

	lgCount := 0
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".lg") {
			lgCount++
		}
	}

	if lgCount != 1 {
		t.Fatalf("there was a wrong number of log files. got=%d", len(files))
	}

	// we don't want to stop the database since that places the queue into sstables.
	// instead we will forcefully shut down the database leaving only the logs.
	db.Alive = false
	// creating a new database reads the log files and recovers the data

	db2 := kantadb.New(kantadb.DefaultConfiguration())
	db2.Run()

	for i := 1; i <= 7; i++ {
		if _, ok := db2.Get("test" + strconv.Itoa(i)); !ok {
			t.Errorf("coult not get value")
		}
	}
}

func TestFilterFileCreation(t *testing.T) {
	db := createTestDatabase(t)
	writeNValues(db, 1e5)

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
}

func TestEverySSTableHasFilter(t *testing.T) {
	db := createTestDatabase(t)
	writeNValues(db, 1e5)

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
}

func TestFullCompaction(t *testing.T) {
	db := createTestDatabase(t)
	writeNValues(db, 8000)

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

	if db.GetTableSize() != 1 {
		t.Errorf("there are more than one sstables after compaction")
	}
}

func TestPartialCompaction(t *testing.T) {
	db := createTestDatabase(t)
	writeNValues(db, 20000)

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
}

func TestFileMerge(t *testing.T) {
	db := createTestDatabase(t)
	db.Run()

	writeNValues(db, 20000)

	// let the sstables write
	time.Sleep(100 * time.Millisecond)

	compactableFiles := db.GetCompactableFiles()

	fmt.Println(compactableFiles)
	// compact the to last files
	filename1 := compactableFiles[len(compactableFiles)-2]
	filename2 := compactableFiles[len(compactableFiles)-1]

	// the merge function assumes that the first file parameter is the more recent one
	if err := db.MergeFiles(filename1, filename2); err != nil {
		t.Errorf("error merging files: %s", err)
	}

	if _, err := os.Stat(filename2); err == nil {
		t.Errorf("the oldest file was not deleted after merging")
	}
}

func TestGetCompactableFiels(t *testing.T) {
	db := createTestDatabase(t)
	db.Run()
	writeNValues(db, 20000)

	// make sure all the sstables are written to disk
	time.Sleep(200 * time.Millisecond)

	compactableFiles := db.GetCompactableFiles()

	if len(compactableFiles) != db.GetTableSize() {
		t.Errorf("amount of compactable files does not equal the amount of sstables. compactable=%d file_count=%d",
			len(compactableFiles), db.GetTableSize())
	}
}
