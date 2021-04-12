package kantadb

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/nireo/kantadb/mem"
	"github.com/nireo/kantadb/sstable"
)

// DB represents the database as a whole.
type DB struct {
	Alive bool
	MEM   *mem.MEM

	// sstables
	ssMutex  sync.Mutex
	SSTables []*sstable.SSTable

	// configuration
	maxMEMsize int
	ssdir      string

	// queue
	MEMQueue   []*mem.MEM
	queueMutex sync.Mutex
}

// NewDB returns a instance of a database given a storage directory for sstables.
// TODO: add some better way of configuring some of the values.
func NewDB(storageDir string) *DB {
	return &DB{
		Alive:      false,
		MEM:        mem.New(),
		SSTables:   make([]*sstable.SSTable, 0),
		ssdir:      storageDir,
		queueMutex: sync.Mutex{},
		maxMEMsize: 1000,
		ssMutex:    sync.Mutex{},
	}
}

// Run starts the db service and starts checking for queue and other things
func (db *DB) Run() error {
	db.Alive = true

	// parse the starting directory for table files containing sstables
	if err := db.parseSSTableDirectory(); err != nil {
		return fmt.Errorf("could not parse sstables or create directory for them: %s", err)
	}

	// start checking for in-memory tables in the queue and start converting in-memory
	// tables into sstables.
	go db.handleQueue()

	return nil
}

// Get tries to find the wanted key from the in-memory table, and if not found checks
// it then checks the queue for the queue.
func (db *DB) Get(key string) (string, bool) {
	val, ok := db.MEM.Get(key)

	if !ok {
		// find from the write queue
		for _, mem := range db.MEMQueue {
			val, ok = mem.Get(key)
			if ok {
				return val, ok
			}
		}
	}

	// if that value still hasn't been found search in the sstables.
	if !ok {
		// note that we start searching from the newest sstable
		for _, st := range db.SSTables {
			val, ok = st.Get(key)
			if ok {
				return val, ok
			}
		}
	}

	// If a value is not found, the value is equal to "" and the ok will be false
	return val, ok
}

// Put writes a value into the in-memory table and also checks if the amount of items
// in the in-memory table exceeds the amount specified in the database configuation.
// If the number is exceeded, add the in-memory table to the start of the queue.
func (db *DB) Put(key, val string) {
	if db.MEM.Size() > db.maxMEMsize {
		db.queueMutex.Lock()

		// add the new in-memory table to the beginning of the list, such that we
		// can easily go through the latest elements when querying and also write older
		// tables to disk
		db.MEMQueue = append([]*mem.MEM{db.MEM}, db.MEMQueue...)
		db.queueMutex.Unlock()

		db.MEM = mem.New()
	}

	// Write key into the plain in-memory table.
	db.MEM.Put(key, val)
}

// HandleQueue takes care of emptying the queue and writing the queue into
// sstables.
func (db *DB) handleQueue() {
	for db.Alive {
		// don't add new tables while dumping the queues to disk
		db.queueMutex.Lock()

		// start from the end because the first element in the array contains the
		// newest items. So we want to add priority to older tables.
		for i := len(db.MEMQueue) - 1; i >= 0; i++ {
			timestamp := time.Now().UnixNano()

			db.ssMutex.Lock()
			sst := sstable.NewSSTable(strconv.FormatInt(timestamp, 10) + ".tb")

			// create the new file
			file, err := os.Create(db.ssdir + "/" + sst.Filename)
			if err != nil {
				// error happended skip this and try again on the next iteration
				continue
			}
			defer file.Close()

			entrs := db.MEMQueue[i].ConvertIntoEntries()
			for _, e := range entrs {
				file.Write(e.ToBinary())
			}

			// now just append the newest sstable to the beginning of the queue
			db.SSTables = append([]*sstable.SSTable{sst}, db.SSTables...)
			db.ssMutex.Unlock()
		}

		// clean up the queue since we went through each item
		db.MEMQueue = []*mem.MEM{}

		db.queueMutex.Unlock()
		time.Sleep(time.Second)
	}
}

// parseSSTableDirectory finds all of the sstable files and adds them to the list.
func (db *DB) parseSSTableDirectory() error {
	paths, err := ioutil.ReadDir(db.ssdir)
	if err != nil {
		// create the ss table directory

		if err := os.Mkdir(db.ssdir, 0600); err != nil {
			return err
		}

		return nil
	}

	var pathStrings []string
	for _, path := range paths {
		pathStrings = append(pathStrings, path.Name())
	}

	sort.Strings(pathStrings)

	var sstlbs []*sstable.SSTable
	for _, path := range pathStrings {
		sst := sstable.NewSSTable(path)
		sstlbs = append([]*sstable.SSTable{sst}, sstlbs...)
	}

	// all of the tables are parsed now we need to create some kind of
	// sparsing index for all of the sstables, but that will come later.
	return nil
}
