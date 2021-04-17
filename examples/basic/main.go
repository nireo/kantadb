package main

import (
	"log"

	"github.com/nireo/kantadb"
)

func main() {
	db := kantadb.New(kantadb.DefaultConfiguration())
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
