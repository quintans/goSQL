package common

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/quintans/goSQL/db"
	tk "github.com/quintans/toolkit"
	"github.com/quintans/toolkit/ext"

	"time"
)

type Dto struct {
	Name      string
	OtherName string
	Value     float64
}

type PublisherSales struct {
	Id           int64
	Name         string
	ThisYear     float64
	PreviousYear float64
}

func (p *PublisherSales) String() string {
	if p == nil {
		return "<nil>"
	}
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", p.Id)
	sb.Add(", Name: ", p.Name)
	sb.Add(", ThisYear: ", p.ThisYear)
	sb.Add(", PreviousYear: ", p.PreviousYear)
	sb.Add("}")
	return sb.String()
}

// since entities can be at the left outer join side, every Id field SHOULD be a pointer
type EntityBase struct {
	Id      *int64
	Version int64
}

// PUBLISHER

// mandatory if we want to reuse entities
var _ tk.Hasher = &Publisher{}

type Publisher struct {
	EntityBase

	Name  *string
	Books []*Book
}

func (p *Publisher) PostInsert(store db.IDb) {
	logger.Infof("===> PostInsert trigger for Publisher with Id %v", *p.Id)
}

func (p *Publisher) PostRetrieve(store db.IDb) {
	logger.Infof("===> PostRetrieve trigger for %s", p.String())
}

func (p *Publisher) String() string {
	if p == nil {
		return "<nil>"
	}
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", p.Id, ", Version: ", p.Version)
	sb.Add(", Name: ", p.Name)
	sb.Add(", Books: ", p.Books)
	sb.Add("}")
	return sb.String()
}

func (p *Publisher) Equals(e interface{}) bool {
	if p == e {
		return true
	}

	switch t := e.(type) {
	case *Publisher:
		return p.Id != nil && t.Id != nil && *p.Id == *t.Id
	}
	return false
}

func (p *Publisher) HashCode() int {
	result := tk.HashType(tk.HASH_SEED, p)
	result = tk.HashLong(result, ext.DefInt64(p.Id, 0))
	return result
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

// BOOK_BIN

// mandatory if we want to reuse entities
var _ tk.Hasher = &Book{}

type BookBin struct {
	EntityBase

	Hardcover []byte
	Book      *Book
}

func (b *BookBin) String() string {
	if b == nil {
		return "<nil>"
	}
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", b.Id, ", Version: ", b.Version)
	sb.Add(", Hardcover: ")
	if len(b.Hardcover) == 0 {
		sb.Add("<nil>")
	} else {
		sb.Add("[]byte")
	}
	sb.Add("}")
	return sb.String()
}

func (b *BookBin) Equals(e interface{}) bool {
	if b == e {
		return true
	}

	switch t := e.(type) {
	case *BookBin:
		return b.Id != nil && t.Id != nil && *b.Id == *t.Id
	}
	return false
}

func (b *BookBin) HashCode() int {
	result := tk.HashType(tk.HASH_SEED, b)
	result = tk.HashLong(result, ext.DefInt64(b.Id, 0))
	return result
}

var (
	BOOK_BIN             = db.TABLE("BOOK_BIN")
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
	db.Marker
	EntityBase

	Name        string
	Price       float64
	Published   *time.Time
	PublisherId *int64
	Publisher   *Publisher // this is filled is a join fetch
	Authors     []*Author
	BookBin     *BookBin

	Title *string
}

func (b *Book) SetName(name string) {
	b.Name = name
	b.Mark("Name")
}

func (b *Book) SetPrice(price float64) {
	b.Price = price
	b.Mark("Price")
}

func (b *Book) SetPublished(published *time.Time) {
	b.Published = published
	b.Mark("Published")
}

func (b *Book) SetPublisherId(id *int64) {
	b.PublisherId = id
	b.Mark("PublisherId")
}

func (b *Book) String() string {
	if b == nil {
		return "nil"
	}
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", b.Id, ", Version: ", b.Version)
	sb.Add(", Name: ", b.Name)
	sb.Add(", Price: ", b.Price)
	sb.Add(", Published: ", b.Published)
	sb.Add(", PublisherId: ", b.PublisherId)
	sb.Add(", Publisher: ", b.Publisher)
	sb.Add(", Title: ", b.Title)
	sb.Add("}")
	return sb.String()
}

func (b *Book) Equals(e interface{}) bool {
	if b == e {
		return true
	}

	switch t := e.(type) {
	case *Book:
		return b.Id != nil && t.Id != nil && *b.Id == *t.Id
	}
	return false
}

func (b *Book) HashCode() int {
	result := tk.HashType(tk.HASH_SEED, b)
	result = tk.HashLong(result, ext.DefInt64(b.Id, 0))
	return result
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

	BOOK_A_AUTHORS = db.NewM2MAssociation(
		"Authors",
		db.ASSOCIATE(BOOK_C_ID).WITH(AUTHOR_BOOK_C_BOOK_ID),
		db.ASSOCIATE(AUTHOR_BOOK_C_AUTHOR_ID).WITH(AUTHOR_C_ID),
	)

	BOOK_A_BOOK_BIN = BOOK.
			ASSOCIATE(BOOK_C_ID).
			TO(BOOK_BIN_C_ID).
			As("BookBin")

	BOOK_A_BOOK_I18N = BOOK.
				ASSOCIATE(BOOK_C_ID).
				TO(BOOK_I18N_C_BOOK_ID).
				As("I18n").
				With(BOOK_I18N_C_LANG, db.Param("lang"))
)

var (
	BOOK_I18N           = db.TABLE("BOOK_I18N")
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
	AUTHOR_BOOK             = db.TABLE("AUTHOR_BOOK")
	AUTHOR_BOOK_C_AUTHOR_ID = AUTHOR_BOOK.KEY("AUTHOR_ID") // implicit map to field 'AuthorId'
	AUTHOR_BOOK_C_BOOK_ID   = AUTHOR_BOOK.KEY("BOOK_ID")   // implicit map to field 'BookId'
)

// AUTHOR

// mandatory if we want to reuse entities
var _ tk.Hasher = &Author{}

type Author struct {
	EntityBase

	Name   *string
	Books  []*Book
	Secret *string `sql:"omit"`
}

func (a *Author) String() string {
	if a == nil {
		return "<nil>"
	}
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", a.Id, ", Version: ", a.Version)
	sb.Add(", Name: ", a.Name)
	sb.Add("}")
	return sb.String()
}

func (a *Author) Equals(e interface{}) bool {
	if a == e {
		return true
	}

	switch t := e.(type) {
	case *Author:
		return a.Id != nil && t.Id != nil && *a.Id == *t.Id
	}
	return false
}

func (a *Author) HashCode() int {
	result := tk.HashType(tk.HASH_SEED, a)
	result = tk.HashLong(result, ext.DefInt64(a.Id, 0))
	return result
}

var (
	AUTHOR           = db.TABLE("AUTHOR")
	AUTHOR_C_ID      = AUTHOR.KEY("ID")
	AUTHOR_C_VERSION = AUTHOR.VERSION("VERSION")
	AUTHOR_C_NAME    = AUTHOR.COLUMN("NAME")
	AUTHOR_C_SECRET  = AUTHOR.COLUMN("SECRET")

	AUTHOR_A_BOOKS = db.NewM2MAssociation(
		"Books",
		db.ASSOCIATE(AUTHOR_C_ID).WITH(AUTHOR_BOOK_C_AUTHOR_ID),
		db.ASSOCIATE(AUTHOR_BOOK_C_BOOK_ID).WITH(BOOK_C_ID),
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

func (p *Project) String() string {
	if p == nil {
		return "<nil>"
	}
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", p.Id, ", Version: ", p.Version)
	sb.Add(", Name: ", p.Name)
	sb.Add(", ManagerId: ", p.ManagerId)
	sb.Add(", ManagerType: ", p.ManagerType)
	sb.Add(", Employee: ", p.Employee)
	sb.Add(", Consultant: ", p.Consultant)
	sb.Add(", StatusCod: ", p.StatusCod)
	sb.Add(", Status: ", p.Status)
	sb.Add("}")
	return sb.String()
}

func (p *Project) Equals(e interface{}) bool {
	if p == e {
		return true
	}

	switch t := e.(type) {
	case *Project:
		return p.Id != nil && t.Id != nil && *p.Id == *t.Id
	}
	return false
}

func (p *Project) HashCode() int {
	result := tk.HashType(tk.HASH_SEED, p)
	result = tk.HashLong(result, ext.DefInt64(p.Id, 0))
	return result
}

var (
	PROJECT                = db.TABLE("PROJECT")
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

	FirstName *string
	LastName  *string

	Project *Project
}

func (p *Employee) String() string {
	if p == nil {
		return "<nil>"
	}
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", p.Id, ", Version: ", p.Version)
	sb.Add(", FirstName: ", p.FirstName)
	sb.Add(", FirstName: ", p.LastName)
	sb.Add(", Project: ", p.Project)
	sb.Add("}")
	return sb.String()
}

func (p *Employee) Equals(e interface{}) bool {
	if p == e {
		return true
	}

	switch t := e.(type) {
	case *Employee:
		return p.Id != nil && t.Id != nil && *p.Id == *t.Id
	}
	return false
}

func (p *Employee) HashCode() int {
	result := tk.HashType(tk.HASH_SEED, p)
	result = tk.HashLong(result, ext.DefInt64(p.Id, 0))
	return result
}

var (
	EMPLOYEE              = db.TABLE("EMPLOYEE")
	EMPLOYEE_C_ID         = EMPLOYEE.KEY("ID")            // implicit map to field Id
	EMPLOYEE_C_VERSION    = EMPLOYEE.VERSION("VERSION")   // implicit map to field Version
	EMPLOYEE_C_FIRST_NAME = EMPLOYEE.COLUMN("FIRST_NAME") // implicit map to field FirstName
	EMPLOYEE_C_LAST_NAME  = EMPLOYEE.COLUMN("LAST_NAME")  // implicit map to field LastName

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

func (c *Consultant) String() string {
	if c == nil {
		return "<nil>"
	}
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", c.Id, ", Version: ", c.Version)
	sb.Add(", Name: ", c.Name)
	sb.Add(", Project: ", c.Project)
	sb.Add("}")
	return sb.String()
}

func (c *Consultant) Equals(e interface{}) bool {
	if c == e {
		return true
	}

	switch t := e.(type) {
	case *Consultant:
		return c.Id != nil && t.Id != nil && *c.Id == *t.Id
	}
	return false
}

func (c *Consultant) HashCode() int {
	result := tk.HashType(tk.HASH_SEED, c)
	result = tk.HashLong(result, ext.DefInt64(c.Id, 0))
	return result
}

var (
	CONSULTANT           = db.TABLE("CONSULTANT")
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
	CATALOG           = db.TABLE("CATALOG")
	CATALOG_C_ID      = CATALOG.KEY("ID")          // implicit map to field Id
	CATALOG_C_VERSION = CATALOG.VERSION("VERSION") // implicit map to field Version
	CATALOG_C_DOMAIN  = CATALOG.COLUMN("DOMAIN")
	CATALOG_C_CODE    = CATALOG.COLUMN("KEY")
	CATALOG_C_VALUE   = CATALOG.COLUMN("VALUE")
)

// STATUS

// mandatory if we want to reuse entities
var _ tk.Hasher = &Status{}

type Status struct {
	EntityBase

	Code        *string
	Description *string
}

func (s *Status) String() string {
	if s == nil {
		return "nil"
	}
	sb := tk.NewStrBuffer()
	sb.Add("{Id: ", s.Id, ", Version: ", s.Version)
	sb.Add(", Code: ", s.Code)
	sb.Add(", Description: ", s.Description)
	sb.Add("}")
	return sb.String()
}

func (s *Status) Equals(e interface{}) bool {
	if s == e {
		return true
	}

	switch t := e.(type) {
	case *Status:
		return s.Id != nil && t.Id != nil && *s.Id == *t.Id
	}
	return false
}

func (s *Status) HashCode() int {
	result := tk.HashType(tk.HASH_SEED, s)
	result = tk.HashLong(result, ext.DefInt64(s.Id, 0))
	return result
}

var (
	STATUS               = db.TABLE("CATALOG").With("DOMAIN", "STATUS")
	STATUS_C_ID          = STATUS.KEY("ID")          // implicit map to field Id
	STATUS_C_VERSION     = STATUS.VERSION("VERSION") // implicit map to field Version
	STATUS_C_CODE        = STATUS.COLUMN("KEY").As("Code")
	STATUS_C_DESCRIPTION = STATUS.COLUMN("VALUE").As("Description")
)

type Palette struct {
	EntityBase

	Code  string
	Value *Color `sql:"converter=color"`
}

type Color struct {
	Red   int
	Green int
	Blue  int
}

/*
func (c *Color) Value() (driver.Value, error) {
	return fmt.Sprintf("%d|%d|%d", c.Red, c.Green, c.Blue), nil
}

func (c *Color) Scan(src interface{}) error {
	s := src.(string)
	rgb := strings.Split(s, "|")
	r, _ := strconv.Atoi(rgb[0])
	g, _ := strconv.Atoi(rgb[1])
	b, _ := strconv.Atoi(rgb[2])
	c.Red = r
	c.Green = g
	c.Blue = b
	return nil
}
*/

type ColorConverter struct{}

func (cc ColorConverter) ToDb(in interface{}) (interface{}, error) {
	if in == nil {
		return in, nil
	}
	c := in.(*Color)
	return fmt.Sprintf("%d|%d|%d", c.Red, c.Green, c.Blue), nil
}

func (cc ColorConverter) FromDbInstance() interface{} {
	var s string
	return &s
}

func (cc ColorConverter) FromDb(in interface{}) (interface{}, error) {
	if in == nil {
		return in, nil
	}

	s := in.(*string)
	rgb := strings.Split(*s, "|")
	r, _ := strconv.Atoi(rgb[0])
	g, _ := strconv.Atoi(rgb[1])
	b, _ := strconv.Atoi(rgb[2])
	c := &Color{}
	c.Red = r
	c.Green = g
	c.Blue = b
	return &c, nil
}

type Supervisor struct {
	EntityBase

	FullName FullNameVO `sql:"embeded"`
}

type FullNameVO struct {
	firstName string
	lastName  string
}

type Supervisor2 struct {
	EntityBase

	FullName *FullNameVO `sql:"embeded"`
}
