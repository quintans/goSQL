package test

import (
	. "github.com/quintans/goSQL/db"
	tk "github.com/quintans/toolkit"
	. "github.com/quintans/toolkit/ext"

	"time"
)

type Dto struct {
	Name      string
	OtherName string
	Value     float64
}

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

func (this *Publisher) String() string {
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", this.Id, ", Version: ", this.Version)
	sb.Add(", Name: ", this.Name)
	sb.Add(", Books: ", this.Books)
	sb.Add("}")
	return sb.String()
}

func (this *Publisher) Equals(e interface{}) bool {
	if this == e {
		return true
	}

	switch t := e.(type) {
	case *Publisher:
		return this.Id != nil && t.Id != nil && *this.Id == *t.Id
	}
	return false
}

func (this *Publisher) HashCode() int {
	result := tk.HashType(tk.HASH_SEED, this)
	result = tk.HashLong(result, DefInt64(this.Id, 0))
	return result
}

var (
	PUBLISHER           = TABLE("PUBLISHER")
	PUBLISHER_C_ID      = PUBLISHER.KEY("ID")          // implicit map to field Id
	PUBLISHER_C_VERSION = PUBLISHER.VERSION("VERSION") // implicit map to field Version
	PUBLISHER_C_NAME    = PUBLISHER.COLUMN("NAME")     // implicit map to field Name

	PUBLISHER_A_BOOKS = PUBLISHER.
				ASSOCIATE(PUBLISHER_C_ID).
				TO(BOOK_C_PUBLISHER_ID).
				As("Books")
)

// BOOK_BIN

// mandatory if we want to reuse entities
var _ tk.Hasher = &Book{}

type BookBin struct {
	EntityBase

	Hardcover []byte
	Book      *Book
}

func (this *BookBin) String() string {
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", this.Id, ", Version: ", this.Version)
	sb.Add(", Hardcover: ")
	if len(this.Hardcover) == 0 {
		sb.Add("<nil>")
	} else {
		sb.Add("[]byte")
	}
	sb.Add("}")
	return sb.String()
}

func (this *BookBin) Equals(e interface{}) bool {
	if this == e {
		return true
	}

	switch t := e.(type) {
	case *BookBin:
		return this.Id != nil && t.Id != nil && *this.Id == *t.Id
	}
	return false
}

func (this *BookBin) HashCode() int {
	result := tk.HashType(tk.HASH_SEED, this)
	result = tk.HashLong(result, DefInt64(this.Id, 0))
	return result
}

var (
	BOOK_BIN             = TABLE("BOOK_BIN")
	BOOK_BIN_C_ID        = BOOK_BIN.KEY("ID")
	BOOK_BIN_C_VERSION   = BOOK_BIN.VERSION("VERSION")
	BOOK_BIN_C_HARDCOVER = BOOK_BIN.COLUMN("HARDCOVER")

	BOOK_BIN_A_BOOK = BOOK_BIN.
			ASSOCIATE(BOOK_BIN_C_ID).
			TO(BOOK_C_ID).
			As("Book")
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
	Authors     []*Author
	BookBin     *BookBin
}

func (this *Book) String() string {
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", this.Id, ", Version: ", this.Version)
	sb.Add(", Name: ", this.Name)
	sb.Add(", Price: ", this.Price)
	sb.Add(", Published: ", this.Published)
	sb.Add(", PublisherId: ", this.PublisherId)
	sb.Add(", Publisher: ", this.Publisher)
	sb.Add("}")
	return sb.String()
}

func (this *Book) Equals(e interface{}) bool {
	if this == e {
		return true
	}

	switch t := e.(type) {
	case *Book:
		return this.Id != nil && t.Id != nil && *this.Id == *t.Id
	}
	return false
}

func (this *Book) HashCode() int {
	result := tk.HashType(tk.HASH_SEED, this)
	result = tk.HashLong(result, DefInt64(this.Id, 0))
	return result
}

var (
	BOOK                = TABLE("BOOK")
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

	BOOK_A_AUTHORS = NewM2MAssociation(
		"Authors",
		ASSOCIATE(BOOK_C_ID).WITH(AUTHOR_BOOK_C_BOOK_ID),
		ASSOCIATE(AUTHOR_BOOK_C_AUTHOR_ID).WITH(AUTHOR_C_ID),
	)

	BOOK_A_BOOK_BIN = BOOK.
			ASSOCIATE(BOOK_C_ID).
			TO(BOOK_BIN_C_ID).
			As("BookBin")
)

// AUTHOR_BOOK

type AuthorBook struct {
	AuthorId *int64
	BookId   *int64
}

var (
	AUTHOR_BOOK             = TABLE("AUTHOR_BOOK")
	AUTHOR_BOOK_C_AUTHOR_ID = AUTHOR_BOOK.KEY("AUTHOR_ID") // implicit map to field 'AuthorId'
	AUTHOR_BOOK_C_BOOK_ID   = AUTHOR_BOOK.KEY("BOOK_ID")   // implicit map to field 'BookId'
)

// AUTHOR

// mandatory if we want to reuse entities
var _ tk.Hasher = &Author{}

type Author struct {
	EntityBase

	Name  *string
	Books []*Book
}

func (this *Author) String() string {
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", this.Id, ", Version: ", this.Version)
	sb.Add(", Name: ", this.Name)
	sb.Add("}")
	return sb.String()
}

func (this *Author) Equals(e interface{}) bool {
	if this == e {
		return true
	}

	switch t := e.(type) {
	case *Author:
		return this.Id != nil && t.Id != nil && *this.Id == *t.Id
	}
	return false
}

func (this *Author) HashCode() int {
	result := tk.HashType(tk.HASH_SEED, this)
	result = tk.HashLong(result, DefInt64(this.Id, 0))
	return result
}

var (
	AUTHOR           = TABLE("AUTHOR")
	AUTHOR_C_ID      = AUTHOR.KEY("ID")
	AUTHOR_C_VERSION = AUTHOR.VERSION("VERSION")
	AUTHOR_C_NAME    = AUTHOR.COLUMN("NAME")

	AUTHOR_A_BOOKS = NewM2MAssociation(
		"Books",
		ASSOCIATE(AUTHOR_C_ID).WITH(AUTHOR_BOOK_C_AUTHOR_ID),
		ASSOCIATE(AUTHOR_BOOK_C_BOOK_ID).WITH(BOOK_C_ID),
	)
)
