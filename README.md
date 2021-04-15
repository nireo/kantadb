# kanta

[![Go Report Card](https://goreportcard.com/badge/github.com/nireo/kantadb)](https://goreportcard.com/report/github.com/nireo/kantadb)

A embeddable database for Go written in pure Go. It is based on LSM-trees meaning that it is more fit for high writes and low reads. The database contains fault-tolerance in the form of logs for in-memory tables and some other things.

## Simple usage

You firstly need to install it:

```
go get -u github.com/nireo/kantadb
```

Then the basic usage is:

```go
package main

import  (
    "log"

    "github.com/nireo/kantadb"
)


func main() {
    db := kantadb.New(kantadb.DefaultOptions())
    defer db.Stop()

    db.Run()

    // put a value
    db.Put("hello", "world")

    // get the value
    value, ok := db.Get("hello")
    if !ok {
        log.Println("value has not been found")
    }

    // delete the value
    db.Delete("hello")
}
```

## How it works

Firstly there is a simple in-memory table, which has a certain max capacity. Once that capacity gets filled, the in-memory table is placed in to a queue, in which it will be written to disk. The queue is checked every 25-100 milliseconds and then if that queue is not empty, tables from the queue are written to disk as sstables.

The combination of in-memory table log files and the sstables makes persistence possible. All the log files and sstables are checked when the database is started up again. Log files are used to make sure in-memory data doesn't get lost if the database suddenly shuts down.
