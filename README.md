goSQL
=====
***
a ORM like library in Go's (golang) that makes it easy to use SQL.

## Introduction

(English is not my native language so please bear with me)

goSQL aims to facilitate the convertion between database tables and structs and make easy
the use of complex joins. 
It has no intention of hiding the SQL from the developer and a closer idiom to SQL is also part of the library.
Structs are used as a representation of a table record for CRUD operations.

This library is not locked to any database vendor. This database abstraction is achieved by what I called translators. Translators for MySQL, PostgreSQL and FirebirdSQL are provided.
These Translators can even be customized and extended even further by registering functions to implement functionality not covered by the initial Translators. Time diff functions come to mind. 

This library is supported by a mapping system that enables you to avoid writing any SQL text. 
References to your database schema are located in one place, avoiding a major pain when you have to refactor your database.

An example of the syntax is as follows:

	var publisher = Publisher{}
	store.Query(PUBLISHER).
		All().
		Where(PUBLISHER_C_ID.Matches(2)).
		SelectTo(&publisher)
		
We are not restricted to the use of structs as demonstrated by the next snippet

	var name *string
	store.Query(PUBLISHER).
		Column(PUBLISHER_C_NAME).
		Where(PUBLISHER_C_ID.Matches(2)).
		SelectTo(&name)

Another example with an update

	store.Update(PUBLISHER).Submit(publisher)


## Features

 - Static typing
 - Result Mapping
 - Database Abstraction
 - Sub Queries
 - Extensible

## Dependencies

Dependes on `database/api`

## Instalation

`go get github.com/quintans/toolkit`

`go get github.com/quintans/goSQL`

## Startup Guide

This guide is based on a MySQL database, so we need to get a database driver.
I used the one in https://github.com/go-sql-driver/mysql 

So lets get started.

Create the table `PUBLISHER` in a MySQL database called `goSQL`.
Of course the database name can be changed and configured to something else.

	CREATE TABLE `PUBLISHER` (
		ID BIGINT NOT NULL AUTO_INCREMENT,
		VERSION INTEGER NOT NULL,
		`NAME` VARCHAR(50),
		`ADDRESS` VARCHAR(255),
		PRIMARY KEY(ID)
	)
	ENGINE=InnoDB 
	DEFAULT CHARSET=utf8;


And the code is

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

The is what you will find in `test/db_test.go`.

## Usage
In this chapter I will try to explain the several aspects of the library using a set of examples.
These examples are supported by tables defined in [tables.sql](test/tables.sql), a MySQL database sql script.

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

	var PUBLISHER = TABLE("PUBLISHER")
	
**Declaring a column**

	var PUBLISHER_C_NAME = PUBLISHER.COLUMN("NAME")     // implicit map to field 'Name''

By default, the result value for this column will be put in the field `Name` of the target struct.
If we wish for a different alias we use the `.As("...")` at the end resulting in:

	var PUBLISHER_C_NAME = PUBLISHER.COLUMN("NAME").As("Other") // map to field 'Other'

The declared alias `Other` is now the default for all the generated SQL. 
As all defaults, it can be changed to another value when building a SQL statement.

Besides the regular columns, there are the special columns `KEY`, `VERSION` and `DELETION`.
	
	var PUBLISHER_C_ID       = PUBLISHER.KEY("ID")           // implicit map to field Id
	var PUBLISHER_C_VERSION  = PUBLISHER.VERSION("VERSION")  // implicit map to field Version
	var PUBLISHER_C_DELETION = PUBLISHER.DELETION("DELETION") // map to field 'Deletion'

- `KEY` identifies the column or columns as primary key of a table.
- `VERSION` identifies the column used for optimistic locking.
- `DELETION` identifies the column used for logic record deletion.

Next we will see how to declare associations. To map associations, we do not think on
the multiplicity of the edges, but how to go from A to B. This leaves us only two types of associations:
Simple (one-to-one, one-to-many, many-to-one) and Composite (many-to-many) associations.
How to use associations is explained in the [Query](#query-examples) chapter.

**Declaring a Simple association**

	var PUBLISHER_A_BOOKS = PUBLISHER.
				ASSOCIATE(PUBLISHER_C_ID).
				TO(BOOK_C_PUBLISHER_ID).
				As("Books")
	
In this example, we see the mapping of the relationship between 
`PUBLISHER` and `BOOK` using the column `PUBLISHER_C_ID` and `BOOK_C_PUBLISHER_ID`.
The `.As("Books")` part indicates that when transforming a query result to a struct, it should follow
the `Books` field to put the transformation part regarding to the `BOOK` entity. 
The Association knows nothing about the multiplicity of its edges.
This association only covers going from `PUBLISHER` to `BOOK`. I we want to go from `BOOK` to `PUBLISHER` we
need to declare the reverse association.

**Declaring an Composite association.**

This kind of associations makes use on an intermediary table, and therefore we need to declare it.

	var (
		AUTHOR_BOOK				= TABLE("AUTHOR_BOOK")
		AUTHOR_BOOK_C_AUTHOR_ID	= AUTHOR_BOOK.KEY("AUTHOR_ID") // implicit map to field 'AuthorId'
		AUTHOR_BOOK_C_BOOK_ID	= AUTHOR_BOOK.KEY("BOOK_ID") // implicit map to field 'BookId'
	)

And finally the Composite association declaration

	var AUTHOR_A_BOOKS = NewM2MAssociation(
		"Books",
		ASSOCIATE(AUTHOR_BOOK_C_AUTHOR_ID).WITH(AUTHOR_C_ID), 
		ASSOCIATE(AUTHOR_BOOK_C_BOOK_ID).WITH(BOOK_C_ID),
	)
 
The order of the parameters is very important, because they indicate the direction of the association.

The full definition of the tables and the struct entities used in this document are in [entities.go](test/entities.go), covering all aspects of table mapping.

### Insert Examples

#### Simple Insert

		insert := Insert(PUBLISHER).
			Columns(PUBLISHER_C_ID, PUBLISHER_C_VERSION, PUBLISHER_C_NAME)
		insert.Values(1, 1, "Geek Publications").Execute()
		insert.Values(2, 1, "Edições Lusas").Execute()

#### Insert With a Struct

When inserting with a struct, the struct fields are matched with the respective columns. 

		var pub Publisher
		pub.Name = ext.StrPtr("Untited Editors")
		store.Insert(PUBLISHER).Submit(pub)

#### Insert Returning Generated Key
Any of the above snippets, if the Id field/column is undefined (0 or nil) it returns the generated key by the database.

		key, _ := store.Insert(PUBLISHER).
			Columns(PUBLISHER_C_ID, PUBLISHER_C_VERSION, PUBLISHER_C_NAME).
			Values(nil, 1, "New Editions").
			Execute()

### Update Examples

#### Update selected columns with Optimistic lock

		store.Update(PUBLISHER).
			Set(PUBLISHER_C_NAME, "Untited Editors"). // column to update
			Set(PUBLISHER_C_VERSION, version + 1).    // increment version
			Where(
			PUBLISHER_C_ID.Matches(1),
			PUBLISHER_C_VERSION.Matches(version),     // old version
		).Execute()

#### Update with struct

When updating with a struct, the struct fields are matched with the respective columns. 
If a version column is present its value is also updated.
The generated SQL will include all columns.

	var publisher Publisher
	publisher.Name = ext.StrPtr("Untited Editors")
	publisher.Id = ext.Int64Ptr(1)      // identifies the record
	publisher.Version = ext.Int64Ptr(1) // for optimistic locking
	store.Update(PUBLISHER).Submit(publisher)
	
#### Update with SubQuery

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


### Delete Examples

#### Simple Delete

	store.Delete(BOOK).Where(BOOK_C_ID.Matches(1)).Execute()

#### Delete with struct

	var book Book // any struct with the fields Id and Version could be used
	book.Id = ext.Int64Ptr(1)
	book.Version = ext.Int64Ptr(1)
	store.Delete(BOOK).Submit(book)


### Query Examples



### Raw SQL

# TODO
- Virtual Columns: Columns that physically exist in another columns
- Test more RDBMS
- Fix code documentation