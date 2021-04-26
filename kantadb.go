package kantadb

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/nireo/kantadb/entries"
	"github.com/nireo/kantadb/mem"
	"github.com/nireo/kantadb/sstable"
	"github.com/nireo/kantadb/utils"
	"github.com/willf/bloom"
)

const MaxMemSize int64 = 1 << 18

// 24 mb
const MaxSSTableSize int64 = 1024 * 1014 * 24

var queueMutex = &sync.Mutex{}
var memMutex = &sync.Mutex{}
var ssMutex = &sync.Mutex{}
var ssListMutex = &sync.Mutex{}
var compactionMutex = &sync.Mutex{}

// DB represents the database as a whole.
type DB struct {
	Alive      bool
	MEM        *mem.MEM
	SSTables   []*sstable.SSTable
	maxMEMsize int
	ssdir      string
	MEMQueue   []*mem.MEM

	writeChan chan *WRequest
	flushChan chan *mem.MEM
}

type WRequest struct {
	Key     string
	Value   string
	errChan chan error
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
		writeChan:  make(chan *WRequest),
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

	// parse all the files related to keeping persistance
	if err := db.parsePersistanceFiles(); err != nil {
		return fmt.Errorf("could not parse directory")
	}

	// // Create a temporary directory for the compaction process
	utils.CreateDirectory(filepath.Join(db.ssdir, "tmp"))

	// we can create a new instance of a memory table since the file directory has been created for sure
	db.MEM = mem.New()

	// start checking for in-memory tables in the queue and start converting in-memory
	// tables into sstables.
	go db.handleQueue()
	go db.handleWrites()

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
		ssMutex.Lock()
		if len(db.SSTables) == 1 {
			val, ok = db.SSTables[0].Get(key)
		} else {
			val, ok = db.concurrentSSTableSearch(key)
		}
		ssMutex.Unlock()
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
func (db *DB) Put(key, val string) error {
	errChan := make(chan error, 1)
	db.writeChan <- &WRequest{
		Key:     key,
		Value:   val,
		errChan: errChan,
	}

	return <-errChan
}

// handleWrites values from the write channel and inserts them into the database,
// it is used in putting values and deleting them.
func (db *DB) handleWrites() {
	for {
		entry := <-db.writeChan
		if err := db.MEM.Put(entry.Key, entry.Value); err != nil {
			entry.errChan <- err
		} else {
			size := db.MEM.Size()
			if size > MaxMemSize {
				queueMutex.Lock()

				// add the new in-memory table to the beginning of the list, such that we
				// can easily go through the latest elements when querying and also write older
				// tables to disk
				db.MEMQueue = append([]*mem.MEM{db.MEM}, db.MEMQueue...)
				queueMutex.Unlock()

				memMutex.Lock()
				db.MEM = mem.New()
				memMutex.Unlock()
			}

			entry.errChan <- nil
		}
	}
}

// Delete has almost exactly the same functionality as read, but instead we set the value of the
// key to a TombstoneValue which just corresponds to a string where the first character is a null-byte.
// We cannot remove the key from the in-memory table since it might reside in the queue or sstable.
// The key will be ultimately deleted when sstable compaction happens.
func (db *DB) Delete(key string) error {
	errChan := make(chan error, 1)
	db.writeChan <- &WRequest{
		Key:     key,
		Value:   entries.TombstoneValue,
		errChan: errChan,
	}

	return <-errChan
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

			ssListMutex.Lock()

			filePath := filepath.Join(db.ssdir, fmt.Sprintf("%v.ss", timestamp))
			sst := sstable.NewSSTable(filePath)

			// create the new file
			file, err := os.Create(sst.Filename)
			if err != nil {
				// error happened skip this and try again on the next iteration
				utils.PrintDebug("error creating sstable: %s", err)
				ssListMutex.Unlock()
				continue
			}
			defer file.Close()

			for _, e := range db.MEMQueue[i].ConvertIntoEntries() {
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

			ssListMutex.Unlock()
		}

		// clean up the queue since we went through each item
		db.MEMQueue = []*mem.MEM{}

		queueMutex.Unlock()
		time.Sleep(time.Millisecond * 100)
	}
}

// parsePersistanceFiles takes in the database directory and parses the log files and the
// sstables and indexes the data in them. The memtables from queues are placed directly to
// disk.
func (db *DB) parsePersistanceFiles() error {
	sstableFiles := utils.ListFilesWithSuffix(db.ssdir, ".ss")
	sort.Strings(sstableFiles)

	logFiles := utils.ListFilesWithSuffix(db.ssdir, ".lg")
	sort.Strings(logFiles)

	var sstlbs []*sstable.SSTable
	for _, path := range sstableFiles {
		sst := sstable.NewSSTable(path)

		// check if that sstable had a filter file
		if err := sst.ParseFilterFromDirectory(); err != nil {
			utils.PrintDebug("could not parse filter file: %s", err)
		}

		sstlbs = append([]*sstable.SSTable{sst}, sstlbs...)
	}

	for _, path := range logFiles {
		table, err := mem.CreateTableFromLog(path)
		if err != nil {
			return err
		}

		queueMutex.Lock()
		db.MEMQueue = append([]*mem.MEM{table}, db.MEMQueue...)
		utils.PrintDebug("read memtable from log file of size: %s", utils.HumanByteCount(table.Size()))
		queueMutex.Unlock()
	}

	db.SSTables = sstlbs

	return nil
}

// concurrentSSTableSearch searches through all the sstables to find the most recent value.
func (db *DB) concurrentSSTableSearch(key string) (string, bool) {
	var (
		val string = ""
		ok  bool   = false
	)

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
	ssListMutex.Lock()
	defer ssListMutex.Unlock()

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

		finalSst.BloomFilter.Add([]byte(key))
		file.Write(entries.KeyValueToBytes(key, value))
	}

	// now that each value has surely been stored we can remove the older files
	// we do this in a seperate loop such that there is no room for problems.

	// btw we need this check since otherwise we cannot really delete all the items without
	// there being problems.

	ssMutex.Lock()
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
	ssMutex.Unlock()

	utils.PrintDebug("currently there are %d sstables", len(db.SSTables))

	utils.PrintDebug("writing compacted filter to disk...")
	if err := finalSst.WriteFilterToDisk(); err != nil {
		return fmt.Errorf("could not write compacted filter file to disk: %s", err)
	}

	utils.PrintDebug("finished compacting %d files. took: %v", n, time.Since(startTime))

	return nil
}

func (db *DB) MergeFiles(f1, f2 string) error {
	// no write or anything on the files for a little while
	ssListMutex.Lock()
	ff1 := filepath.Join(db.ssdir, f1)
	if _, err := utils.CopyFile(ff1, ff1+".tmp"); err != nil {
		ssListMutex.Unlock()
		return fmt.Errorf("could not copy file into into temp file: %s", err)
	}

	ff2 := filepath.Join(db.ssdir, f2)
	if _, err := utils.CopyFile(ff2, ff2+".tmp"); err != nil {
		ssListMutex.Unlock()
		return fmt.Errorf("could not copy file into into temp file: %s", err)
	}
	ssListMutex.Unlock()

	utils.PrintDebug("created temporary merge files at: %s and %s", ff1, ff2)

	values := redblacktree.NewWithStringComparator()
	filesToMerge := []string{ff1 + ".tmp", ff2 + ".tmp"}

	filter := bloom.New(20000, 5)
	for _, filename := range filesToMerge {
		ssFile, err := os.Open(filename)
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

			values.Put(entry.Key, entry.Value)
		}
	}
	utils.PrintDebug("merged temporary file values")

	// assuming that the f1 is the more recent file.
	file, err := os.Create(ff1 + ".tmpf")
	if err != nil {
		return err
	}
	defer file.Close()

	utils.PrintDebug("created the final merge file")

	// the tree iterates through the keys in a sorted fashion
	iter := values.Iterator()
	for iter.Next() {
		key := iter.Key().(string)
		val := iter.Value().(string)

		if val == entries.TombstoneValue {
			// we want to remove tombstone values in the compaction process
			continue
		}

		filter.Add([]byte(key))
		file.Write(entries.KeyValueToBytes(key, val))
	}

	utils.PrintDebug("wrote merged values to file and constructed the bloom filter")

	// deleting all the unneeded files
	toDelete := []string{ff1, ff2, ff1 + ".tmp", ff2 + ".tmp"}
	for _, toDel := range toDelete {
		if err := os.Remove(toDel); err != nil {
			utils.PrintDebug("could not delete file: %s", toDel)
			return err
		}
		utils.PrintDebug("removed file: %s", toDel)
	}

	if err := os.Rename(file.Name(), strings.Replace(file.Name(), ".tmpf", "", -1)); err != nil {
		utils.PrintDebug("could not rename the result file: %s", err)
		return err
	}

	utils.PrintDebug("renamde file final file")

	ssListMutex.Lock()
	utils.PrintDebug("current there are %d sstables", len(db.SSTables))
	deletedFileIndex := db.GetSSTableIndex(ff2)
	replacedFileIndex := db.GetSSTableIndex(ff1)

	utils.PrintDebug("deleting sstable at index: %d; replacing at index: %d", deletedFileIndex, replacedFileIndex)

	ssMutex.Lock()
	db.SSTables[replacedFileIndex] = &sstable.SSTable{
		Filename:    ff1,
		BloomFilter: filter,
	}
	utils.PrintDebug("replaced sstable")

	copy(db.SSTables[deletedFileIndex:], db.SSTables[deletedFileIndex+1:])
	db.SSTables[len(db.SSTables)-1] = nil
	db.SSTables = db.SSTables[:len(db.SSTables)-1]
	ssMutex.Unlock()
	utils.PrintDebug("removed sstable")
	ssListMutex.Unlock()

	return nil
}

// GetSSTableIndex returns the index of the sstable with a given filename
func (db *DB) GetSSTableIndex(filename string) int {
	for i := 0; i < len(db.SSTables); i++ {
		if db.SSTables[i].Filename == filename {
			return i
		}
	}

	return -1
}

// GetCompactableFiles returns all of the files in the SSTable directory that are small enough.
func (db *DB) GetCompactableFiles() []string {
	var res []string

	files, err := ioutil.ReadDir(db.ssdir)
	if err != nil {
		return []string{}
	}

	for _, file := range files {
		// make sure the file is valid
		if file.IsDir() || file.Size() > MaxSSTableSize || !strings.HasSuffix(file.Name(), ".ss") {
			continue
		}
		res = append(res, file.Name())
	}

	// make sure the files are sorted in order such that the oldest files are in the back
	sort.Slice(res, func(i, j int) bool {
		return res[i] > res[j]
	})

	return res
}

// Stop clears the data gracefully from the memtables are sstable write queue
func (db *DB) Stop() error {
	// if we're closing don't accept new queue entries
	queueMutex.Lock()
	defer queueMutex.Unlock()
	db.MEMQueue = append([]*mem.MEM{db.MEM}, db.MEMQueue...)

	ssListMutex.Lock()
	for i := len(db.MEMQueue) - 1; i >= 0; i-- {
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

		entrs := db.MEMQueue[i].ConvertIntoEntries()
		for _, e := range entrs {
			file.Write(e.ToBinary())
		}
	}

	db.MEMQueue = []*mem.MEM{}
	ssListMutex.Unlock()

	db.Alive = false
	return nil
}
