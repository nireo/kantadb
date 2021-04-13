package kantadb_test

import (
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
