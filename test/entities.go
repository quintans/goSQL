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

	Title *string
}

func (this *Book) String() string {
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", this.Id, ", Version: ", this.Version)
	sb.Add(", Name: ", this.Name)
	sb.Add(", Price: ", this.Price)
	sb.Add(", Published: ", this.Published)
	sb.Add(", PublisherId: ", this.PublisherId)
	sb.Add(", Publisher: ", this.Publisher)
	sb.Add(", Title: ", this.Title)
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
	BOOK_C_TITLE        = BOOK.VCOLUMN(BOOK_I18N_C_TITLE, BOOK_A_BOOK_I18N)

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

	BOOK_A_BOOK_I18N = BOOK.
				ASSOCIATE(BOOK_C_ID).
				TO(BOOK_I18N_C_BOOK_ID).
				As("I18n").
				With(BOOK_I18N_C_LANG, Param("lang"))
)

var (
	BOOK_I18N           = TABLE("BOOK_I18N")
	BOOK_I18N_C_ID      = BOOK_I18N.KEY("ID")
	BOOK_I18N_C_VERSION = BOOK_I18N.VERSION("VERSION")
	BOOK_I18N_C_BOOK_ID = BOOK_I18N.COLUMN("BOOK_ID")
	BOOK_I18N_C_LANG    = BOOK_I18N.COLUMN("LANG")
	BOOK_I18N_C_TITLE   = BOOK_I18N.COLUMN("TITLE")
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

// PROJECT

// mandatory if we want to reuse entities
var _ tk.Hasher = &Project{}

type Project struct {
	EntityBase

	Name        *string
	ManagerId   *int64
	ManagerType *string

	Employee   []*Employee
	Consultant []*Consultant
	StatusCod  *string
	Status     *Status
}

func (this *Project) String() string {
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", this.Id, ", Version: ", this.Version)
	sb.Add(", Name: ", this.Name)
	sb.Add(", ManagerId: ", this.ManagerId)
	sb.Add(", ManagerType: ", this.ManagerType)
	sb.Add(", Employee: ", this.Employee)
	sb.Add(", Consultant: ", this.Consultant)
	sb.Add(", StatusCod: ", this.StatusCod)
	sb.Add(", Status: ", this.Status)
	sb.Add("}")
	return sb.String()
}

func (this *Project) Equals(e interface{}) bool {
	if this == e {
		return true
	}

	switch t := e.(type) {
	case *Project:
		return this.Id != nil && t.Id != nil && *this.Id == *t.Id
	}
	return false
}

func (this *Project) HashCode() int {
	result := tk.HashType(tk.HASH_SEED, this)
	result = tk.HashLong(result, DefInt64(this.Id, 0))
	return result
}

var (
	PROJECT                = TABLE("PROJECT")
	PROJECT_C_ID           = PROJECT.KEY("ID")              // implicit map to field Id
	PROJECT_C_VERSION      = PROJECT.VERSION("VERSION")     // implicit map to field Version
	PROJECT_C_NAME         = PROJECT.COLUMN("NAME")         // implicit map to field Name
	PROJECT_C_MANAGER_ID   = PROJECT.COLUMN("MANAGER_ID")   // implicit map to field ManagerId
	PROJECT_C_MANAGER_TYPE = PROJECT.COLUMN("MANAGER_TYPE") // implicit map to field ManagerType
	PROJECT_C_STATUS       = PROJECT.COLUMN("STATUS_COD")   // implicit map to field Status

	PROJECT_A_EMPLOYEE = PROJECT.
				ASSOCIATE(PROJECT_C_MANAGER_ID).
				TO(EMPLOYEE_C_ID).
				As("Employee").
				With(PROJECT_C_MANAGER_TYPE, "E")

	PROJECT_A_CONSULTANT = PROJECT.
				ASSOCIATE(PROJECT_C_MANAGER_ID).
				TO(CONSULTANT_C_ID).
				As("Consultant").
				With(PROJECT_C_MANAGER_TYPE, "C")

	PROJECT_A_STATUS = PROJECT.
				ASSOCIATE(PROJECT_C_STATUS).
				TO(STATUS_C_CODE).
				As("Status")
)

// EMPLOYEE

// mandatory if we want to reuse entities
var _ tk.Hasher = &Employee{}

type Employee struct {
	EntityBase

	Name *string

	Project *Project
}

func (this *Employee) String() string {
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", this.Id, ", Version: ", this.Version)
	sb.Add(", Name: ", this.Name)
	sb.Add(", Project: ", this.Project)
	sb.Add("}")
	return sb.String()
}

func (this *Employee) Equals(e interface{}) bool {
	if this == e {
		return true
	}

	switch t := e.(type) {
	case *Employee:
		return this.Id != nil && t.Id != nil && *this.Id == *t.Id
	}
	return false
}

func (this *Employee) HashCode() int {
	result := tk.HashType(tk.HASH_SEED, this)
	result = tk.HashLong(result, DefInt64(this.Id, 0))
	return result
}

var (
	EMPLOYEE           = TABLE("EMPLOYEE")
	EMPLOYEE_C_ID      = EMPLOYEE.KEY("ID")          // implicit map to field Id
	EMPLOYEE_C_VERSION = EMPLOYEE.VERSION("VERSION") // implicit map to field Version
	EMPLOYEE_C_NAME    = EMPLOYEE.COLUMN("NAME")     // implicit map to field Name

	EMPLOYEE_A_PROJECT = EMPLOYEE.
				ASSOCIATE(EMPLOYEE_C_ID).
				TO(PROJECT_C_MANAGER_ID).
				As("Project").
				With(PROJECT_C_MANAGER_TYPE, "E")
)

// CONSULTANT

// mandatory if we want to reuse entities
var _ tk.Hasher = &Consultant{}

type Consultant struct {
	EntityBase

	Name *string

	Project *Project
}

func (this *Consultant) String() string {
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", this.Id, ", Version: ", this.Version)
	sb.Add(", Name: ", this.Name)
	sb.Add(", Project: ", this.Project)
	sb.Add("}")
	return sb.String()
}

func (this *Consultant) Equals(e interface{}) bool {
	if this == e {
		return true
	}

	switch t := e.(type) {
	case *Consultant:
		return this.Id != nil && t.Id != nil && *this.Id == *t.Id
	}
	return false
}

func (this *Consultant) HashCode() int {
	result := tk.HashType(tk.HASH_SEED, this)
	result = tk.HashLong(result, DefInt64(this.Id, 0))
	return result
}

var (
	CONSULTANT           = TABLE("CONSULTANT")
	CONSULTANT_C_ID      = CONSULTANT.KEY("ID")          // implicit map to field Id
	CONSULTANT_C_VERSION = CONSULTANT.VERSION("VERSION") // implicit map to field Version
	CONSULTANT_C_NAME    = CONSULTANT.COLUMN("NAME")     // implicit map to field Name

	CONSULTANT_A_PROJECT = CONSULTANT.
				ASSOCIATE(CONSULTANT_C_ID).
				TO(PROJECT_C_MANAGER_ID).
				As("Project").
				With(PROJECT_C_MANAGER_TYPE, "C")
)

// CATALOG

var (
	CATALOG               = TABLE("CATALOG")
	CATALOG_C_ID          = CATALOG.KEY("ID")          // implicit map to field Id
	CATALOG_C_VERSION     = CATALOG.VERSION("VERSION") // implicit map to field Version
	CATALOG_C_DOMAIN      = CATALOG.COLUMN("DOMAIN")
	CATALOG_C_CODE        = CATALOG.COLUMN("KEY")
	CATALOG_C_DESCRIPTION = CATALOG.COLUMN("VALUE")
)

// STATUS

// mandatory if we want to reuse entities
var _ tk.Hasher = &Status{}

type Status struct {
	EntityBase

	Code        *string
	Description *string
}

func (this *Status) String() string {
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", this.Id, ", Version: ", this.Version)
	sb.Add(", Code: ", this.Code)
	sb.Add(", Description: ", this.Description)
	sb.Add("}")
	return sb.String()
}

func (this *Status) Equals(e interface{}) bool {
	if this == e {
		return true
	}

	switch t := e.(type) {
	case *Status:
		return this.Id != nil && t.Id != nil && *this.Id == *t.Id
	}
	return false
}

func (this *Status) HashCode() int {
	result := tk.HashType(tk.HASH_SEED, this)
	result = tk.HashLong(result, DefInt64(this.Id, 0))
	return result
}

var (
	STATUS               = TABLE("CATALOG").With("DOMAIN", "STATUS")
	STATUS_C_ID          = STATUS.KEY("ID")          // implicit map to field Id
	STATUS_C_VERSION     = STATUS.VERSION("VERSION") // implicit map to field Version
	STATUS_C_CODE        = STATUS.COLUMN("KEY").As("Code")
	STATUS_C_DESCRIPTION = STATUS.COLUMN("VALUE").As("Description")
)
