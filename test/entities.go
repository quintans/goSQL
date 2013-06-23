package test

import (
	"github.com/quintans/goSQL/db"
	tk "github.com/quintans/toolkit"
	. "github.com/quintans/toolkit/ext"
)

// since entities can be at the left outer join side, every field SHOULD be a pointer
type EntityBase struct {
	Id      *int64
	Version *int64
}

// mandatory if we want to reuse entities
var _ tk.Hasher = &Publisher{}

type Publisher struct {
	EntityBase

	Name *string
	//Books []*Book
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

func (this *Publisher) String() string {
	sb := tk.NewStrBuffer()
	sb.Add("{")
	sb.Add("Id: ", this.Id)
	sb.Add(", Version: ", this.Version)
	sb.Add(", Name: ", this.Name)
	//sb.Add(", Books: ", this.Books)
	sb.Add("}")
	return sb.String()
}

var (
	PUBLISHER           = db.TABLE("PUBLISHER")
	PUBLISHER_C_ID      = PUBLISHER.KEY("ID")          // implicit map to field Id
	PUBLISHER_C_VERSION = PUBLISHER.VERSION("VERSION") // implicit map to field Ver
	PUBLISHER_C_NAME    = PUBLISHER.COLUMN("NAME")     // implicit map to field Name

	/*
		PUBLISHER_A_BOOKS = PUBLISHER.
					ASSOCIATE(PUBLISHER_C_ID).
					TO(BOOK_C_PUBLISHER_ID).
					As("Books")
	*/
)
