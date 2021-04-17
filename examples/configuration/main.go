package main

import (
	"log"

	"github.com/nireo/kantadb"
)

func main() {
	config := &kantadb.Config{
		MaxMemSize: 2048,        // Largest amount of elements stored in the in-memory table.
		Debug:      false,       // print debug information to stdout
		StorageDir: "./storgae", // where tables will be stored
	}

	db := kantadb.New(config)
	defer db.Stop()

	db.Run()

	// put a value
	db.Put("hello", "world")

	// get the value
	value, ok := db.Get("hello")
	if !ok {
		log.Println("value has not been found")
	}

	log.Printf("got value %s", value)

	// delete the value
	db.Delete("hello")
}
