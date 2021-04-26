package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/nireo/kantadb"
)

var amount = flag.Int("amount", 10000, "the amount of items to write and get from the database")

func init() {
	flag.Parse()
}

func main() {
	rand.Seed(time.Now().UnixNano())

	db := kantadb.New(kantadb.DefaultConfiguration())
	db.Run()

	log.Printf("writing %d key-value pairs to database", *amount)
	startTime := time.Now()
	for i := 0; i < *amount; i++ {
		randString := strconv.Itoa(rand.Int())
		if err := db.Put(randString, "val-"+randString); err != nil {
			fmt.Printf("could not write value %s\n", randString)
		}
	}

	log.Printf("took %v to write", time.Since(startTime))

	if err := os.RemoveAll(db.GetDirectory()); err != nil {
		log.Printf("could not delete directory: %s", err)
	}
}
