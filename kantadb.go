package kantadb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nireo/kantadb/entries"
	"github.com/nireo/kantadb/mem"
	"github.com/nireo/kantadb/sstable"
	"github.com/nireo/kantadb/utils"
)

var queueMutex = &sync.Mutex{}
var memMutex = &sync.Mutex{}
var ssMutex = &sync.Mutex{}

// DB represents the database as a whole.
type DB struct {
	Alive      bool
	MEM        *mem.MEM
	SSTables   []*sstable.SSTable
	maxMEMsize int
	ssdir      string
	MEMQueue   []*mem.MEM
}

// ssTableSearch represents a value from the sstables, where the index is the index of sstable.
// The index is used to determine which value is the most fresh.
type ssTableSearch struct {
	index int
	value string
}

// Config represents different parameters to change te default behaviour of the database
type Config struct {
	StorageDir string
	MaxMemSize int
	Debug      bool
}

// DefaultConfiguration returns a database config that has some default values
func DefaultConfiguration() *Config {
	return &Config{
		Debug:      true,
		MaxMemSize: 4096,
		StorageDir: "./kantadb",
	}
}

// New returns a instance of a database given a storage directory for sstables.
func New(config *Config) *DB {
	conf := &Config{}
	if config == nil {
		conf = DefaultConfiguration()
	} else {
		conf = config
	}

	mem.SetLogPath(conf.StorageDir)

	utils.SetDebuggingMode(config.Debug)

	return &DB{
		Alive:      false,
		SSTables:   make([]*sstable.SSTable, 0),
		ssdir:      conf.StorageDir,
		maxMEMsize: conf.MaxMemSize,
	}
}

// GetDirectory returns the directory in which all of the files are stored.
func (db *DB) GetDirectory() string {
	return db.ssdir
}

// Run starts the db service and starts checking for queue and other things
func (db *DB) Run() error {
	utils.PrintDebug("starting the database service...")

	db.Alive = true

	utils.PrintDebug("starting to parse sstables...")
	// parse the starting directory for table files containing sstables
	if err := db.parseSSTableDirectory(); err != nil {
		return fmt.Errorf("could not parse sstables or create directory for them: %s", err)
	}

	// parse for ss directory for log files
	if err := db.parseLogFiles(); err != nil {
		return fmt.Errorf("could not parse log directory or create directory for them: %s", err)
	}

	// we can create a new instance of a memory table since the file directory has been created for sure
	db.MEM = mem.New()

	// start checking for in-memory tables in the queue and start converting in-memory
	// tables into sstables.
	go db.handleQueue()

	return nil
}

// Get tries to find the wanted key from the in-memory table, and if not found checks
// it then checks the queue for the value. If the value is not found in the queue, check
// the sstables. If the value corresponds to a TombstoneValue return a invalid key since
// the key was "deleted".
func (db *DB) Get(key string) (string, bool) {
	memMutex.Lock()
	val, ok := db.MEM.Get(key)
	memMutex.Unlock()

	if !ok {
		// find from the write queue
		for _, mem := range db.MEMQueue {
			val, ok = mem.Get(key)
			if ok {
				break
			}
		}
	}

	// if that value still hasn't been found search in the sstables.
	if !ok {
		if len(db.SSTables) == 1 {
			val, ok = db.SSTables[0].Get(key)
		} else {
			val, ok = db.concurrentSSTableSearch(key)
		}
	}

	// The value shuold be deleted so the value cannot be found
	if val == entries.TombstoneValue {
		return "", false
	}

	// If a value is not found, the value is equal to "" and the ok will be false
	return val, ok
}

// Put writes a value into the in-memory table and also checks if the amount of items
// in the in-memory table exceeds the amount specified in the database configuation.
// If the number is exceeded, add the in-memory table to the start of the queue.
func (db *DB) Put(key, val string) {
	memMutex.Lock()
	size := db.MEM.Size()
	memMutex.Unlock()

	if size > db.maxMEMsize {
		queueMutex.Lock()

		// add the new in-memory table to the beginning of the list, such that we
		// can easily go through the latest elements when querying and also write older
		// tables to disk
		db.MEMQueue = append([]*mem.MEM{db.MEM}, db.MEMQueue...)
		utils.PrintDebug("reset the memory table, length of queue: %d", len(db.MEMQueue))

		queueMutex.Unlock()

		memMutex.Lock()
		db.MEM = mem.New()
		memMutex.Unlock()
	}

	// Write key into the plain in-memory table.
	memMutex.Lock()
	db.MEM.Put(key, val)
	memMutex.Unlock()
}

// Delete has almost exactly the same functionality as read, but instead we set the value of the
// key to a TombstoneValue which just corresponds to a string where the first character is a null-byte.
// We cannot remove the key from the in-memory table since it might reside in the queue or sstable.
// The key will be ultimately deleted when sstable compaction happens.
func (db *DB) Delete(key string) {
	memMutex.Lock()
	size := db.MEM.Size()
	memMutex.Unlock()

	if size > db.maxMEMsize {
		queueMutex.Lock()

		// add the new in-memory table to the beginning of the list, such that we
		// can easily go through the latest elements when querying and also write older
		// tables to disk
		db.MEMQueue = append([]*mem.MEM{db.MEM}, db.MEMQueue...)
		utils.PrintDebug("reset the memory table, length of queue: %d", len(db.MEMQueue))

		queueMutex.Unlock()

		memMutex.Lock()
		db.MEM = mem.New()
		memMutex.Unlock()
	}

	// Write key into the plain in-memory table.
	memMutex.Lock()
	db.MEM.Put(key, entries.TombstoneValue)
	memMutex.Unlock()
}

// HandleQueue takes care of emptying the queue and writing the queue into
// sstables.
func (db *DB) handleQueue() {
	for {
		// don't add new tables while dumping the queues to disk
		queueMutex.Lock()

		// start from the end because the first element in the array contains the
		// newest items. So we want to add priority to older tables.

		for i := len(db.MEMQueue) - 1; i >= 0; i-- {
			timestamp := time.Now().UnixNano()

			ssMutex.Lock()

			filePath := filepath.Join(db.ssdir, fmt.Sprintf("%v.ss", timestamp))
			sst := sstable.NewSSTable(filePath)

			// create the new file
			file, err := os.Create(sst.Filename)
			if err != nil {
				// error happened skip this and try again on the next iteration
				utils.PrintDebug("error creating sstable: %s", err)
				ssMutex.Unlock()
				continue
			}
			defer file.Close()

			entrs := db.MEMQueue[i].ConvertIntoEntries()
			for _, e := range entrs {
				// a bloom filter is just a optimization to that helps finding out
				// if a value has already been seen.
				sst.BloomFilter.Add([]byte(e.Key))
				file.Write(e.ToBinary())
			}

			// after constructing the bloom filter in the previous loop we can just
			// write the bloom filter into a filter file.
			sst.WriteFilterToDisk()

			utils.PrintDebug("created a new sstable at: %s", sst.Filename)

			// now just append the newest sstable to the beginning of the queue
			db.SSTables = append([]*sstable.SSTable{sst}, db.SSTables...)

			ssMutex.Unlock()
		}

		// clean up the queue since we went through each item
		db.MEMQueue = []*mem.MEM{}

		queueMutex.Unlock()
		time.Sleep(time.Millisecond * 100)
	}
}

// parseLogFiles goes through the log directory and creates memory tables for each log.
// We parse the log files since, if the database unexpectedly shuts down we can recover
// the data.
func (db *DB) parseLogFiles() error {
	fileNames := utils.ListFilesWithSuffix(db.ssdir, ".lg")

	// we want create memtables and append them to the queue
	for _, path := range fileNames {
		table, err := mem.CreateTableFromLog(path)
		if err != nil {
			return err
		}

		queueMutex.Lock()
		db.MEMQueue = append([]*mem.MEM{table}, db.MEMQueue...)
		utils.PrintDebug("read memtable from log file of size: %d", table.Size())
		queueMutex.Unlock()
	}

	// this will be ran before starting the queue flusher such that queue can instantly
	// clear queue and convert these tables into sstables.

	return nil
}

// parseSSTableDirectory finds all of the sstable files and adds them to the list.
func (db *DB) parseSSTableDirectory() error {
	fileNames := utils.ListFilesWithSuffix(db.ssdir, ".ss")
	sort.Strings(fileNames)

	var sstlbs []*sstable.SSTable
	for _, path := range fileNames {
		sst := sstable.NewSSTable(path)

		// check if that sstable had a filter file
		if err := sst.ParseFilterFromDirectory(); err != nil {
			utils.PrintDebug("could not parse filter file: %s", err)
		}

		sstlbs = append([]*sstable.SSTable{sst}, sstlbs...)
	}

	utils.PrintDebug("found %d sstables", len(sstlbs))

	// no need to use mutex since this code isn't ran concurrently
	db.SSTables = sstlbs

	// all of the tables are parsed now we need to create some kind of
	// sparsing index for all of the sstables, but that will come later.
	return nil
}

// concurrentSSTableSearch searches through all the sstables to find the most recent value.
func (db *DB) concurrentSSTableSearch(key string) (string, bool) {
	var (
		val string = ""
		ok  bool   = false
	)

	// no new sstables while we're searching them
	ssMutex.Lock()

	itemsChan := make(chan ssTableSearch)
	wg := &sync.WaitGroup{}

	wg.Add(len(db.SSTables))

	for index, table := range db.SSTables {
		go func(i int, tbl *sstable.SSTable) {
			defer wg.Done()

			val, ok := tbl.Get(key)
			if ok {
				itemsChan <- ssTableSearch{
					index: i,
					value: val,
				}
			}
		}(index, table)
	}

	// go through all of the sstables. we need to go through them to get the most recent result.
	wg.Wait()
	close(itemsChan)

	mostRecentIndex := len(db.SSTables)
	for item := range itemsChan {
		if item.index <= mostRecentIndex {
			val = item.value
			ok = true
			mostRecentIndex = item.index
		}
	}

	ssMutex.Unlock()

	return val, ok
}

// GetTableSize returns the amount of sstables indexed
func (db *DB) GetTableSize() int {
	return len(db.SSTables)
}

// compactNTables combines the last n sstables.
func (db *DB) CompactNTables(n int) error {
	// to make sure the file timestamp is not a false representation we use the
	// most recently compacted sstables timestamp

	if n > len(db.SSTables) {
		return errors.New("the number of tables to compact exceed number of tables")
	}

	// This is used to display debugging information
	startTime := time.Now()

	// make sure there are no changes made into the files.
	ssMutex.Lock()

	var filename string
	finalValues := make(map[string]string)

	utils.PrintDebug("from %d sstables compacting %d", len(db.SSTables), n)
	for i := len(db.SSTables) - 1; i >= len(db.SSTables)-n; i-- {
		filename = db.SSTables[i].Filename + ".tmp"
		ssFile, err := os.Open(db.SSTables[i].Filename)
		if err != nil {
			return fmt.Errorf("could not compact file: %s", err)
		}
		defer ssFile.Close()

		// create a entry reader to read all values from the files
		entryScanner := entries.InitScanner(ssFile, 4096)

		for {
			entry, err := entryScanner.ReadNext()
			if err != nil {
				break
			}

			finalValues[entry.Key] = entry.Value
		}
	}

	utils.PrintDebug("creating compacted result file...")
	finalSst := sstable.NewSSTable(filename)
	// write the final and most up-to-date values to the new file.
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	for key, value := range finalValues {
		// if the most updated value for given key is equal to a tombstone block
		// meaning the key has been deleted. So don't include it.
		if value == entries.TombstoneValue {
			continue
		}

		entry := &entries.Entry{
			Type:  entries.KVPair,
			Key:   key,
			Value: value,
		}

		finalSst.BloomFilter.Add([]byte(key))
		file.Write(entry.ToBinary())
	}

	// now that each value has surely been stored we can remove the older files
	// we do this in a seperate loop such that there is no room for problems.

	// btw we need this check since otherwise we cannot really delete all the items without
	// there being problems.
	if len(db.SSTables) == n {
		utils.PrintDebug("deleting all sstables...")
		db.SSTables = []*sstable.SSTable{}

		for _, file := range db.SSTables {
			utils.PrintDebug("deleting sstable %s", file.Filename)
			if err := os.Remove(file.Filename); err != nil {
				return err
			}

			if err := os.Remove(file.GetFilterFilename()); err != nil {
				return err
			}
		}
	} else {
		originalLength := len(db.SSTables)
		for i := originalLength - 1; i >= originalLength-n; i-- {
			utils.PrintDebug("deleting sstable %s at index %d", db.SSTables[i].Filename, i)
			if err := os.Remove(db.SSTables[i].Filename); err != nil {
				return err
			}

			if err := os.Remove(db.SSTables[i].GetFilterFilename()); err != nil {
				return err
			}

			copy(db.SSTables[i:], db.SSTables[i+1:])
			db.SSTables[len(db.SSTables)-1] = nil
			db.SSTables = db.SSTables[:len(db.SSTables)-1]
		}
	}

	utils.PrintDebug("moving tmp file as permanent")
	// remove the tmp prefix from the compacted file
	if err := os.Rename(filename, strings.Replace(filename, ".tmp", "", -1)); err != nil {
		return err
	}
	finalSst.Filename = strings.Replace(filename, ".tmp", "", -1)
	db.SSTables = append([]*sstable.SSTable{finalSst}, db.SSTables...)

	utils.PrintDebug("currently there are %d sstables", len(db.SSTables))

	utils.PrintDebug("writing compacted filter to disk...")
	if err := finalSst.WriteFilterToDisk(); err != nil {
		return fmt.Errorf("could not write compacted filter file to disk: %s", err)
	}

	ssMutex.Unlock()
	utils.PrintDebug("finished compacting %d files. took: %v", n, time.Since(startTime))

	return nil
}

// Stop clears the data gracefully from the memtables are sstable write queue
func (db *DB) Stop() error {
	queueMutex.Lock()
	db.MEMQueue = append([]*mem.MEM{db.MEM}, db.MEMQueue...)
	queueMutex.Unlock()

	timestamp := time.Now().UnixNano()
	filePath := filepath.Join(db.ssdir, fmt.Sprintf("%v.sstable", timestamp))
	sst := sstable.NewSSTable(filePath)

	// create the new file
	file, err := os.Create(sst.Filename)
	if err != nil {
		// error happened skip this and try again on the next iteration
		utils.PrintDebug("error creating sstable: %s", err)
		return fmt.Errorf("could not write remaining memtable: %s", err)
	}
	defer file.Close()

	entrs := db.MEMQueue[0].ConvertIntoEntries()
	for _, e := range entrs {
		file.Write(e.ToBinary())
	}

	db.Alive = false
	return nil
}
