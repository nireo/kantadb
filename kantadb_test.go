package kantadb_test

import (
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nireo/kantadb"
)

func TestFolderCreated(t *testing.T) {
	db := kantadb.New("./testfolder")

	// start running the database services
	db.Run(true)

	if _, err := os.Stat("./testfolder"); os.IsNotExist(err) {
		t.Errorf("could not create new directory")
	}

	// remove the newly generated folder
	if err := os.Remove("./testfolder"); err != nil {
		log.Printf("could not delete database folder")
	}
}

func TestBasicMemoryOperations(t *testing.T) {
	db := kantadb.New("./testfolder")
	db.Run(true)

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
	if err := os.Remove("./testfolder"); err != nil {
		log.Printf("could not delete database folder")
	}
}

func TestAllGets(t *testing.T) {
	db := kantadb.New("./testfolder")
	db.Run(true)

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
	if err := os.RemoveAll("./testfolder"); err != nil {
		log.Printf("could not delete database folder")
	}
}

func TestSSTableCreation(t *testing.T) {
	db := kantadb.New("./testfolder")
	db.Run(true)

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 4000; i++ {
		randomNumber := rand.Int()
		db.Put(strconv.Itoa(randomNumber), "value-"+strconv.Itoa(randomNumber))
	}

	// wait for all of the sstables to go through to disk
	time.Sleep(time.Millisecond * 200)

	sstables, err := ioutil.ReadDir("./testfolder")
	if err != nil {
		t.Errorf("could not find files in the testfolder: %s", err)
	}

	// since the maximum size of the memory table is 1028 so there should be 3 sstables created.
	if len(sstables) == 0 {
		t.Errorf("there were no files in the directory")
	}

	if err := os.RemoveAll("./testfolder"); err != nil {
		t.Errorf("could not delete database folder")
	}
}

func TestPersistance(t *testing.T) {
	db1 := kantadb.New("./testfolder")
	db1.Run(true)

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

	db2 := kantadb.New("./testfolder")
	db2.Run(true)

	for _, key := range stored {
		if _, ok := db2.Get(key); !ok {
			t.Errorf("error getting key: %s", key)
		}
	}

	if err := os.RemoveAll("./testfolder"); err != nil {
		t.Errorf("could not delete database folder")
	}
}

func TestDelete(t *testing.T) {
	db := kantadb.New("./testfolder")
	db.Run(true)

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

	if err := os.RemoveAll("./testfolder"); err != nil {
		t.Errorf("error removing database folder: %s", err)
	}
}
