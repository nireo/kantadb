# kanta

[![Go Report Card](https://goreportcard.com/badge/github.com/nireo/kantadb)](https://goreportcard.com/report/github.com/nireo/kantadb)

A embeddable database for Go written in pure Go. It is based on LSM-trees meaning that it is more fit for high writes and low reads. The database contains fault-tolerance in the form of logs for in-memory tables and some other things.
