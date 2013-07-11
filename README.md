# goSQL
***

a ORM like library in Go's (golang) that makes it easy to use SQL.

## Introduction

(English is not my native language so please bear with me)

goSQL aims to facilitate the convertion between database tables and structs and make easy
the use of complex joins. 
It has no intention of hiding the SQL from the developer and a closer idiom to SQL is also part of the library.  
Structs can be used as a representation of a table record for CRUD operations but there is no direct dependency between a struct and a table. It is the field of a struct that is matched with the column alias of SQL statement to build a result. 

This library is not locked to any database vendor. This database abstraction is achieved by what I called _Translators_. Translators for MySQL, PostgreSQL and FirebirdSQL are provided.
These Translators can be extended  by registering functions to implement functionality not covered by the initial Translators or customize to something specific to a project. 

This library is supported by a mapping system that enables you to avoid writing any SQL text. 
References to your database schema are located in one place, avoiding a major pain when you have to refactor your database.

An example of the syntax is as follows:

```go
var publisher = Publisher{}
store.Query(PUBLISHER).
	All().
	Where(PUBLISHER_C_ID.Matches(2)).
	SelectTo(&publisher)
```
		
We are not restricted to the use of structs as demonstrated by the next snippet
	
```go
var name *string
store.Query(PUBLISHER).
	Column(PUBLISHER_C_NAME).
	Where(PUBLISHER_C_ID.Matches(2)).
	SelectInto(&name)
```

Another example with an update

```go
store.Update(PUBLISHER).Submit(publisher)
```

## Features

 - Static typing
 - Result Mapping
 - Database Abstraction
 - Joins made easy
 - Result Pagination
 - Extensible

## Instalation

`go get github.com/quintans/toolkit`

`go get github.com/quintans/goSQL`

## Startup Guide

This guide is based on a MySQL database, so we need to get a database driver.
I used the one in https://github.com/go-sql-driver/mysql 

So lets get started.

Create the table `PUBLISHER` in a MySQL database called `goSQL`.
Of course the database name can be changed and configured to something else.

```sql
CREATE TABLE `PUBLISHER` (
	ID BIGINT NOT NULL AUTO_INCREMENT,
	VERSION INTEGER NOT NULL,
	`NAME` VARCHAR(50),
	`ADDRESS` VARCHAR(255),
	PRIMARY KEY(ID)
)
ENGINE=InnoDB 
DEFAULT CHARSET=utf8;
```

And the code is

```go
import (
	. "github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/dbx"
	trx "github.com/quintans/goSQL/translators"

	_ "github.com/go-sql-driver/mysql"

	"database/sql"
	"fmt"
)

// the entity
type Publisher struct {
	Id      *int64
	Version *int64
	Name    *string
}

// table description/mapping
var (
	PUBLISHER           = TABLE("PUBLISHER")
	PUBLISHER_C_ID      = PUBLISHER.KEY("ID")          // implicit map to field Id
	PUBLISHER_C_VERSION = PUBLISHER.VERSION("VERSION") // implicit map to field Version
	PUBLISHER_C_NAME    = PUBLISHER.COLUMN("NAME")     // implicit map to field Name
)

// the transaction manager
var TM ITransactionManager

func init() {
	// database configuration	
	mydb, err := sql.Open("mysql", "root:root@/goSQL?parseTime=true")
	if err != nil {
		panic(err)
	}

	// transaction manager	
	TM = NewTransactionManager(
		// database
		mydb,
		// database context factory
		func(c dbx.IConnection) IDb {
			return NewDb(c, trx.NewMySQL5Translator())
		},
		// statement cache
		1000,
	)
}

func main() {
	// get the databse context
	store := TM.Store()
	// the target entity
	var publisher = Publisher{}
	
	_, err := store.Query(PUBLISHER).
		All().
		Where(PUBLISHER_C_ID.Matches(2)).
		SelectTo(&publisher)
		
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s", publisher)		
}
```

The is what you will find in [test/db_test.go](test/db_test.go).

## Usage
In this chapter I will try to explain the several aspects of the library using a set of examples.
These examples are supported by tables defined in [test/tables.sql](test/tables.sql), a MySQL database sql script.

Before diving in to the examples I first describe the table model and how to map the entities.

### Entity Relation Diagram 
![ER Diagram](test/er.png)

Relationships explained:
- **One-to-Many**: One Publisher can have many Books and one Book has one Publisher. 
- **One-to-One**: One Book has one Book_Bin (Hardcover) - binary data is stored in separated in different table - and one Book_Bin has one Book.
- **Many-to-Many**: One Author can have many Books and one Book can have many Authors

### Table definition

As seen in the [Startup Guide](#startup-guide), mapping a table is pretty straight forward.

**Declaring a table**

```go
var PUBLISHER = TABLE("PUBLISHER")
```
	
**Declaring a column**

```go
var PUBLISHER_C_NAME = PUBLISHER.COLUMN("NAME")     // implicit map to field 'Name''
```

By default, the result value for this column will be put in the field `Name` of the target struct.
If we wish for a different alias we use the `.As("...")` at the end resulting in:

```go
var PUBLISHER_C_NAME = PUBLISHER.COLUMN("NAME").As("Other") // map to field 'Other'
```

The declared alias `Other` is now the default for all the generated SQL. 
As all defaults, it can be changed to another value when building a SQL statement.

Besides the regular columns, there are the special columns `KEY`, `VERSION` and `DELETION`.
	
```go
var PUBLISHER_C_ID       = PUBLISHER.KEY("ID")           // implicit map to field Id
var PUBLISHER_C_VERSION  = PUBLISHER.VERSION("VERSION")  // implicit map to field Version
var PUBLISHER_C_DELETION = PUBLISHER.DELETION("DELETION") // map to field 'Deletion'
```

- `KEY` identifies the column or columns as primary key of a table.
- `VERSION` identifies the column used for optimistic locking.
- `DELETION` identifies the column used for logic record deletion.

It is not mandatory to map all columns of a table. For the same physical table several logical tables can be created with diferent set of columns. They can even refer to diferent domain values depending on a discriminator column as seen in the [Table Discriminator](#table-discriminator) section.

Next we will see how to declare associations. To map associations, we do not think on
the multiplicity of the edges, but how to go from A to B. This leaves us only two types of associations:
Simple (one-to-one, one-to-many, many-to-one) and Composite (many-to-many) associations.
How to use associations is explained in the [Query](#query-examples) chapter.

**Declaring a Simple association**

```go
var PUBLISHER_A_BOOKS = PUBLISHER.
			ASSOCIATE(PUBLISHER_C_ID).
			TO(BOOK_C_PUBLISHER_ID).
			As("Books")
```
	
In this example, we see the mapping of the relationship between 
`PUBLISHER` and `BOOK` using the column `PUBLISHER_C_ID` and `BOOK_C_PUBLISHER_ID`.
The `.As("Books")` part indicates that when transforming a query result to a struct, it should follow
the `Books` field to put the transformation part regarding to the `BOOK` entity. 
The Association knows nothing about the multiplicity of its edges.
This association only covers going from `PUBLISHER` to `BOOK`. I we want to go from `BOOK` to `PUBLISHER` we
need to declare the reverse association.

**Declaring an Composite association.**

This kind of associations makes use on an intermediary table, and therefore we need to declare it.

```go
var (
	AUTHOR_BOOK				= TABLE("AUTHOR_BOOK")
	AUTHOR_BOOK_C_AUTHOR_ID	= AUTHOR_BOOK.KEY("AUTHOR_ID") // implicit map to field 'AuthorId'
	AUTHOR_BOOK_C_BOOK_ID	= AUTHOR_BOOK.KEY("BOOK_ID") // implicit map to field 'BookId'
)
```

And finally the Composite association declaration

```go
var AUTHOR_A_BOOKS = NewM2MAssociation(
	"Books",
	ASSOCIATE(AUTHOR_BOOK_C_AUTHOR_ID).WITH(AUTHOR_C_ID), 
	ASSOCIATE(AUTHOR_BOOK_C_BOOK_ID).WITH(BOOK_C_ID),
)
```
 
The order of the parameters is very important, because they indicate the direction of the association.

The full definition of the tables and the struct entities used in this document are in [test/entities.go](test/entities.go), covering all aspects of table mapping.

### Insert Examples

#### Simple Insert

```go
insert := Insert(PUBLISHER).
	Columns(PUBLISHER_C_ID, PUBLISHER_C_VERSION, PUBLISHER_C_NAME)
insert.Values(1, 1, "Geek Publications").Execute()
insert.Values(2, 1, "Edições Lusas").Execute()
```

There is another way of supplying values, for all CRUD operations, and this is by parameter,
as seen in the following snippet.

```go
insert.SetParameter("name", "Geek Publications")
insert.Values(1, 1, Param("name")).Execute()
```

In this example the value for the `name` parameter is directly supplied in the snippet but it could be an "environment" variable supplied by a custom `store` for every CRUD operation.  
One example, could be `language` (pt, eng, ...) for internationalized text, or `channel` (web, mobile, ...) for descriptions, etc.


#### Insert With a Struct

When inserting with a struct, the struct fields are matched with the respective columns. 

```go
var pub Publisher
pub.Name = ext.StrPtr("Untited Editors")
store.Insert(PUBLISHER).Submit(pub)
```

#### Insert Returning Generated Key
Any of the above snippets, if the Id field/column is undefined (0 or nil) it returns the generated key by the database.

```go
key, _ := store.Insert(PUBLISHER).
	Columns(PUBLISHER_C_ID, PUBLISHER_C_VERSION, PUBLISHER_C_NAME).
	Values(nil, 1, "New Editions").
	Execute()
```

### Update Examples

#### Update selected columns with Optimistic lock

```go
store.Update(PUBLISHER).
	Set(PUBLISHER_C_NAME, "Untited Editors"). // column to update
	Set(PUBLISHER_C_VERSION, version + 1).    // increment version
	Where(
	PUBLISHER_C_ID.Matches(1),
	PUBLISHER_C_VERSION.Matches(version),     // old version
).Execute()
```

#### Update with struct

When updating with a struct, the struct fields are matched with the respective columns. 
If a version column is present its value is also updated.
The generated SQL will include all columns.

```go
var publisher Publisher
publisher.Name = ext.StrPtr("Untited Editors")
publisher.Id = ext.Int64Ptr(1)      // identifies the record
publisher.Version = ext.Int64Ptr(1) // for optimistic locking
store.Update(PUBLISHER).Submit(publisher)
```
	
#### Update with SubQuery

This example shows of subquery to do an update, and also the use of `Exists`.

```go
sub := store.Query(BOOK).Alias("b").
	Column(AsIs(nil)).
	Where(
	BOOK_C_PUBLISHER_ID.Matches(Col(BOOK_C_ID).For("a")),
	BOOK_C_PRICE.Greater(10),
)

affectedRows, err := store.Update(PUBLISHER).Alias("a").
	Set(PUBLISHER_C_NAME, Upper(PUBLISHER_C_NAME)).
	Where(Exists(sub)).
	Execute()
```

### Delete Examples

#### Simple Delete

```go
store.Delete(BOOK).Where(BOOK_C_ID.Matches(2)).Execute()
```

#### Delete with struct

```go
var book Book // any struct with the fields Id and Version could be used
book.Id = ext.Int64Ptr(2)
book.Version = ext.Int64Ptr(1)
store.Delete(BOOK).Submit(book)
```

### Query Examples

The query operation is by far the richest operation of the ones we have seen.  
Query operation that start with _Select*_ retrive one result, and  those that start with _List*_ return a list of results.

#### SelectInto

The result of the query is put in the supplied variables. Can be a value or a pointer.

```go
var name string
ok, err := store.Query(PUBLISHER).
	Column(PUBLISHER_C_NAME).
	Where(PUBLISHER_C_ID.Matches(2)).
	SelectInto(&name)
```

#### SelectTo

The result of the query is put in the supplied struct.

```go
var publisher Publisher
store.Query(PUBLISHER).
	All().
	Where(PUBLISHER_C_ID.Matches(2)).
	SelectTo(&publisher)
```

#### SelectTreeTo
Executes the query and builds a struct tree.

The first parameter must be a struct pointer.

If the reuse parameter is true, when a
new entity is needed, the cache is checked to see if there is one instance for this entity,
and if found it will use it to build the tree. Because of this the supplied instance
must implement the toolkit.Hasher interface.

If the reuse parameter is false, each element of the tree is always a new instance
even if representing the same entity. This is most useful for tabular results.
Since there is no need for caching the entities it is not mandatory to implement
the toolkit.Hasher interface.

The result of the query is put in the supplied struct.  
Returns true if a result was found, false if no result

```go
var publisher Publisher
store.Query(PUBLISHER).
	All().
	Outer(PUBLISHER_A_BOOKS).
	Fetch(). // add all columns off book in the query
	Where(PUBLISHER_C_ID.Matches(2)).
	SelectTreeTo(&publisher, true)
```

#### SelectTree

Executes the query and returns a struct tree.

Regarding the parameter `reuse` the behavior is the same as SelectTreeTo.

```go
result := store.Query(PUBLISHER).
	All().
	Outer(PUBLISHER_A_BOOKS).
	Fetch(). // add all columns off book in the query
	Where(PUBLISHER_C_ID.Matches(2)).
	SelectTree((*Publisher)(nil), true)
publisher := result.(*Publisher)	
```

#### LisSimpleFor

Lists simple variables.  
A closure is used to build the result list.
The types for scanning are supplied by the instances parameter.

```go
names := make([]string, 0)
var name string
err := store.Query(PUBLISHER).
	Column(PUBLISHER_C_NAME).
	ListSimpleFor(func() {
	names = append(names, name)
}, &name)
```

#### ListFor

Executes a query where the target entity is created by a supplied factory closure.
Supplying the entity at the start, avoid us of having to cast the result from a generic result, as we see later.

```go
books := make([]*Book, 0) // mandatory use pointers
err := store.Query(BOOK).
	All().
	ListFor(func() interface{} {
	book := new(Book)
	books = append(books, book)
	return book
})
```

#### ListOf

Another way of executing the above query would be

```go
books, err := store.Query(BOOK).
	All().
	ListOf((*Book)(nil))
```

ListOf returns a `collection.Collection` interface. 
The reason to use a new data structure instead of the classic slices, was the need, in some cases, for returning a result where the an entity had to be unique, as we will see later on, and finding an instance in a hash collection is faster than finding it in a slice. 
For `collection.Collection` implementations that require an hashable elements, the struct instance to be added to the collection must implement the `toolkit.Hasher` interface, as is the case of Book.

To traverse the results we use the following code

```go
for e := books.Enumerator(); e.HasNext(); {
	book := e.Next().(*Book)
	// do something
}
```

#### ListFlatTreeFor

Executes a query and flushes the result into the instance supplied by the factory function.
The factory only supplies the head of the tree.
A new instance is created for every returned row.  
The responsability of building the result is delegated to the factory function.

```go
publishers := make([]*Publisher, 0)
store.Query(PUBLISHER).
	All().
	Outer(PUBLISHER_A_BOOKS).
	Fetch(). // add all columns off book in the query
	Where(PUBLISHER_C_ID.Matches(2)).
	ListFlatTreeFor(func() interface{} {
	publisher := new(Publisher)
	publishers = append(publishers, publisher)
	return publisher
})
```

#### ListTreeOf

Executes a query and transform the results to the struct type.
It matches the alias with struct property name, building a struct tree.
If the transformed data matches a previous converted entity the previous one is reused.

Receives a template instance and returns a collection of structs.

```go
publishers, _ := store.Query(PUBLISHER).
	All().
	Outer(PUBLISHER_A_BOOKS).
	Fetch(). // add all columns off book in the query
	Where(PUBLISHER_C_ID.Matches(2)).
	ListTreeOf((*Publisher)(nil))
	
for e := publishers.Enumerator(); e.HasNext(); {
	publisher := e.Next().(*Publisher)
	// do something here
}
```

#### Column Subquery

For this example we will use the following struct which will hold the result for each row.
Notice that the struct does not represent any table.

```go
type Dto struct {
	Name      string
	OtherName string
	Value     float64
}
```

The following query gets the name of the publisher and the sum of the prices of books for each publisher (using a subquery to sum), building the result as a slice of `Sale`.

```go
subquery := store.Query(BOOK).Alias("b").
	Column(Sum(BOOK_C_PRICE)).
	Where(
	BOOK_C_PUBLISHER_ID.Matches(Col(PUBLISHER_C_ID).For("p")),
)

var dtos = make([]*Dto, 0)
store.Query(PUBLISHER).Alias("p").
	Column(PUBLISHER_C_NAME).
	Column(subquery).As("Value").
	ListFor(func() interface{} {
	dto := new(Dto)
	dtos = append(dtos, dto)
	return dto
})
```

Notice that when I use the subquery variable an alias with the value “Value” is defined. This alias matches with a struct field in `Dto`. In this query the TArtist.C_NAME column as no associated alias, so the default column alias is used.

#### Where Subquery 

In this example I get a list of records with the name of the Publisher, the name and price of every Book, where the price is lesser or equal than 10. The result is put in a slice of Sale instances.  
For this a subquery used in the where clause.

```go
subquery := store.Query(BOOK).
	Distinct().
	Column(BOOK_C_PUBLISHER_ID).
	Where(
	BOOK_C_PRICE.LesserOrMatch(10),
)

var dtos = make([]*Dto, 0)
store.Query(PUBLISHER).
	Column(PUBLISHER_C_NAME).
	Inner(PUBLISHER_A_BOOKS).
	Include(BOOK_C_NAME).As("OtherName").
	Include(BOOK_C_PRICE).As("Value").
	Join().
	Where(PUBLISHER_C_ID.In(subquery)).
	ListFor(func() interface{} {
	dto := new(Dto)
	dtos = append(dtos, dto)
	return dto
})
```

#### Joins

The concepts of joins was already introduced in the section [SelectTreeTo](#selectTreeTo) where we can see the use of an outer join. 
Joins can be seen has a chain of associations that extend from the main table to a target table. Along the way we can apply constraints and/or include columns from the participating tables. This chains can overlap without problem because they are seen as isolated from one another.
Joins can be `Outer` or `Inner` and can have constraints applyied to the target table of the last added asscoiation through the use of the function `On()`.
To delimite the joins we can use the function `Join()` or `Fetch()`. Both process the join but the later includes in the query all columns from all the tables of the joins. `Fetch()`is used when a struct tree is desired.

Ex: list all publishers that had a book published before 2013

```go
var publishers = make([]*Publisher, 0)
store.Query(PUBLISHER).
	All().
	Distinct().
	Inner(PUBLISHER_A_BOOKS).
	On(BOOK_C_PUBLISHED.Lesser(time.Date(2013, time.January, 1, 0, 0, 0, 0, time.UTC))).
	Join().
	ListFor(func() interface{} {
	publisher := new(Publisher)
	publishers = append(publishers, publisher)
	return publisher
})
```

The section [Where Subquery](#where-subquery) also shows the use of `Include`.

The next example executes an (left) outer join and includes **ALL** columns of the participating tables in the join. The result is a collection of `*Publisher` structs with its childs in tree.

```go
result, err := store.Query(PUBLISHER).
	All().
	Outer(PUBLISHER_A_BOOKS, BOOK_A_AUTHORS).
	Fetch().
	ListTreeOf((*Publisher)(nil))
```

### Group By

For this example I will use the struct defined in [Column Subquery](#column-subquery).

```go
var dtos = make([]*Dto, 0)
store.Query(PUBLISHER).
	Column(PUBLISHER_C_NAME).
	Outer(PUBLISHER_A_BOOKS).
	IncludeToken(Sum(BOOK_C_PRICE)).As("Value").
	Join().
	GroupByPos(1).
	ListFor(func() interface{} {
	dto := new(Dto)
	dtos = append(dtos, dto)
	return dto
})
```

### Order By

List all publishers, ordering ascending by name.

```go
var publishers = make([]*Publisher, 0)
store.Query(PUBLISHER).
	All().
	OrderBy(PUBLISHER_C_NAME).
	Asc(true).
	ListFor(func() interface{} {
	publisher := new(Publisher)
	publishers = append(publishers, publisher)
	return publisher
})
```

It is possible to add more orders, and even to order by columns belonging to other tables if joins were present.


### Union

### Pagination

To paginate the results of a query we use the windowing functions `Skip` and `Limit`.

```go
var publishers = make([]*Publisher, 0)
store.Query(PUBLISHER).
	All().
	Outer(PUBLISHER_A_BOOKS, BOOK_A_AUTHORS).
	Fetch().
	Order(PUBLISHER_C_NAME).Asc(true).
	Skip(2).  // skip the first 2 records
	Limit(3). // returns next 3 records
	ListFlatTreeFor(func() interface{} {
	publisher := new(Publisher)
	publishers = append(publishers, publisher)
	return publisher
})
```

### Association Discriminator

An exclusive OR relationship indicates that entity A is related to either entity B or entity C but not both B and C. This is implemented by defining associations with a constraint.

For the example I will use the following database schema:

![ER Diagram](test/er2.png)


As seen in [test/entities.go](test/entities.go) associations of this type are described as:

```go
PROJECT_A_CONSULTANT = PROJECT.
			ASSOCIATE(PROJECT_C_MANAGER_ID).
			TO(CONSULTANT_C_ID).
			As("Consultant").
			With(PROJECT_C_MANAGER_TYPE, "C")
```

where the function `With` declares the constraint applied to this association.

With this in place, its use is the same as regular associations.

```go
store.Query(PROJECT).
	All().
	Inner(PROJECT_A_EMPLOYEE).
	Fetch().
	Order(PROJECT_C_NAME).Asc(true).
	ListTreeOf((*Project)(nil))
```

### Table Discriminator

When mapping a table it is possible to declare that the domain of that table only refers to a subset of values of the physical table. This is done by defining a restriction (Discriminator) at the table definition.  
With this we avoid of having to write a **where** condition every time we want to refer to a specific domain.  
Inserts will automatically apply the discriminator.

To demonstrate this I will use a physical table named CATALOG that can hold unrelated information, like gender, eye color, etc.  
The creation script and table definitions for the next example are at [test/tables.sql](test/tables.sql) and [test/entities.go](test/entities.go) respectively.

```go
statuses := make([]*Status, 0)
store.Query(STATUS).
	All().
	ListFor(func() interface{} {
	status := new(Status)
	statuses = append(statuses, status)
	return status
})
```


### Virtual Columns

Virtual columns are columns declared in a table but in reality they belong to another table. These tables are related by a one-to-one association, with constraints guaranteeing the one-to-one relationship.  
**Virtual columns are only used by queries**.  
The columns are intended to resolve the case where the column value depends on the environment. For example, internationalization, were the value of the column would depend on the language. Another application is the case where we would like to have different descriptions depending on the business client that accesses the data, for example, mobile or web.  
Let’s use an internationalization example.


### Custom Functions

### Raw SQL

```go
result := make([]string, 0)
dba.QueryClosure("select col1 from tab1 where col2 = ?", func(rows *sql.Rows) error {
	var col1 string
	if err := rows.Scan(&col1); err != nil {
		return err
	}
	result = append(result, col1)
	return nil
}, param1)
```

# Credits


# TODO
- Add more tests
- Virtual Columns: Columns that physically exist in another columns
- Test more RDBMS
- Fix code documentation