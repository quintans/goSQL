package test

import (
	"github.com/quintans/goSQL/db"
	tk "github.com/quintans/toolkit"
	. "github.com/quintans/toolkit/ext"

	"time"
)

// since entities can be at the left outer join side, every field SHOULD be a pointer
type EntityBase struct {
	Id      *int64
	Version *int64
}

// PUBLISHER

// mandatory if we want to reuse entities
var _ tk.Hasher = &Publisher{}

type Publisher struct {
	EntityBase

	Name  *string
	Books []*Book
}

func (this *Publisher) Equals(e interface{}) bool {
	if this == e {
		return true
	}

	switch t := e.(type) {
	case *Publisher:
		return this.Id == t.Id
	}
	return false
}

func (this *Publisher) HashCode() int {
	return tk.HashLong(tk.HASH_SEED, DefInt64(this.Id, 0))
}

var (
	PUBLISHER           = db.TABLE("PUBLISHER")
	PUBLISHER_C_ID      = PUBLISHER.KEY("ID")          // implicit map to field Id
	PUBLISHER_C_VERSION = PUBLISHER.VERSION("VERSION") // implicit map to field Version
	PUBLISHER_C_NAME    = PUBLISHER.COLUMN("NAME")     // implicit map to field Name

	PUBLISHER_A_BOOKS = PUBLISHER.
				ASSOCIATE(PUBLISHER_C_ID).
				TO(BOOK_C_PUBLISHER_ID).
				As("Books")
)

// BOOK

// mandatory if we want to reuse entities
var _ tk.Hasher = &Book{}

type Book struct {
	EntityBase

	Name        *string
	Price       *float64
	Published   *time.Time
	PublisherId *int64
	Publisher   *Publisher // this is filled is a join fetch
}

func (this *Book) Equals(e interface{}) bool {
	if this == e {
		return true
	}

	switch t := e.(type) {
	case *Book:
		return this.Id == t.Id
	}
	return false
}

func (this *Book) HashCode() int {
	return tk.HashLong(tk.HASH_SEED, DefInt64(this.Id, 0))
}

var (
	BOOK                = db.TABLE("BOOK")
	BOOK_C_ID           = BOOK.KEY("ID")
	BOOK_C_VERSION      = BOOK.VERSION("VERSION")
	BOOK_C_NAME         = BOOK.COLUMN("NAME")
	BOOK_C_PRICE        = BOOK.COLUMN("PRICE")
	BOOK_C_PUBLISHED    = BOOK.COLUMN("PUBLISHED")
	BOOK_C_PUBLISHER_ID = BOOK.COLUMN("PUBLISHER_ID")

	BOOK_A_PUBLISHER = BOOK.
				ASSOCIATE(BOOK_C_PUBLISHER_ID).
				TO(PUBLISHER_C_ID).
				As("Publisher")
)
