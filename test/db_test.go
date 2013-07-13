package test

import (
	. "github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/dbx"
	trx "github.com/quintans/goSQL/translators"
	"github.com/quintans/toolkit/ext"
	"github.com/quintans/toolkit/log"

	_ "bitbucket.org/miquella/mgodbc" // float64 was fixed acording to issue #5
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"database/sql"
	"fmt"
	"testing"
	"time"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/test")

// custom Db - for setting default parameters
func NewMyDb(connection dbx.IConnection, translator Translator, lang string) *MyDb {
	return &MyDb{&Db{connection, translator}, lang}
}

type MyDb struct {
	*Db
	Lang string
}

func (this *MyDb) Query(table *Table) *Query {
	query := NewQuery(this, table)
	query.SetParameter("lang", this.Lang)
	return query
}

const TOKEN_SECONDSDIFF = "SECONDSDIFF"

// SecondsDiff Token factory
// first parameter is greater than the second
func SecondsDiff(left, right interface{}) *Token {
	return NewToken(TOKEN_SECONDSDIFF, left, right)
}

func init() {
	log.Register("/", log.DEBUG, log.NewConsoleAppender(false))
}

var RAW_SQL string

func InitMySQL5() (ITransactionManager, *sql.DB) {
	RAW_SQL = "SELECT `name` FROM `book` WHERE `name` LIKE ?"

	translator := trx.NewMySQL5Translator()
	/*
		registering custom function.
		A custom translator could be created instead.
	*/
	translator.RegisterTranslation(
		TOKEN_SECONDSDIFF,
		func(dmlType DmlType, token Tokener, tx Translator) string {
			m := token.GetMembers()
			return fmt.Sprintf(
				"TIME_TO_SEC(TIMEDIFF(%s, %s))",
				tx.Translate(dmlType, m[0]),
				tx.Translate(dmlType, m[1]),
			)
		},
	)

	return InitDB("mysql", "root:root@/ezsql?parseTime=true", translator)
}

func InitPostgreSQL() (ITransactionManager, *sql.DB) {
	RAW_SQL = "SELECT name FROM book WHERE name LIKE $1"

	translator := trx.NewPostgreSQLTranslator()
	translator.RegisterTranslation(
		TOKEN_SECONDSDIFF,
		func(dmlType DmlType, token Tokener, tx Translator) string {
			m := token.GetMembers()
			return fmt.Sprintf(
				"EXTRACT(EPOCH FROM (%s - %s))",
				tx.Translate(dmlType, m[0]),
				tx.Translate(dmlType, m[1]),
			)
		},
	)

	return InitDB("postgres", "dbname=postgres user=postgres password=postgres sslmode=disable", translator)
}

func InitFirebirdSQL() (ITransactionManager, *sql.DB) {
	RAW_SQL = "SELECT name FROM book WHERE name LIKE ?"

	translator := trx.NewFirebirdSQLTranslator()
	translator.RegisterTranslation(
		TOKEN_SECONDSDIFF,
		func(dmlType DmlType, token Tokener, tx Translator) string {
			m := token.GetMembers()
			return fmt.Sprintf(
				"DATEDIFF(SECOND, %s, %s)",
				tx.Translate(dmlType, m[1]),
				tx.Translate(dmlType, m[0]),
			)
		},
	)

	return InitDB("mgodbc", "dsn=FirebirdEZSQL;uid=SYSDBA;pwd=masterkey", translator)
}

func InitDB(driverName, dataSourceName string, translator Translator) (ITransactionManager, *sql.DB) {
	mydb, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		panic(err)
	}

	// wake up the database pool
	err = mydb.Ping()
	if err != nil {
		panic(err)
	}

	return NewTransactionManager(
		// database
		mydb,
		// databse context factory
		func(c dbx.IConnection) IDb {
			//return db.NewDb(c, trx.NewFirebirdSQLTranslator())
			//return NewDb(c, trx.NewMySQL5Translator())
			return NewMyDb(c, translator, "pt")
		},
		// statement cache
		1000,
	), mydb
}

const (
	PUBLISHER_UTF8_NAME = "Edições Lusas"
	LANG                = "pt"
	BOOK_LANG_TITLE     = "Era uma vez..."
)

func TestAll(t *testing.T) {
	tm, theDB := InitFirebirdSQL()
	RunAll(tm, t)
	theDB.Close()

	tm, theDB = InitPostgreSQL()
	RunAll(tm, t)
	theDB.Close()

	tm, theDB = InitMySQL5()
	RunAll(tm, t)
	theDB.Close()
}

func RunAll(TM ITransactionManager, t *testing.T) {
	RunSelectUTF8(TM, t)
	RunInsertReturningKey(TM, t)
	RunInsertStructReturningKey(TM, t)
	RunSimpleUpdate(TM, t)
	RunStructUpdate(TM, t)
	RunUpdateSubquery(TM, t)
	RunSimpleDelete(TM, t)
	RunStructDelete(TM, t)
	RunSelectInto(TM, t)
	RunSelectTreeTo(TM, t)
	RunSelectTree(TM, t)
	RunListFor(TM, t)
	RunListOf(TM, t)
	RunListFlatTreeFor(TM, t)
	RunListTreeOf(TM, t)
	RunListSimpleFor(TM, t)
	RunColumnSubquery(TM, t)
	RunWhereSubquery(TM, t)
	RunInnerOn(TM, t)
	RunInnerOn2(TM, t)
	RunOuterFetch(TM, t)
	RunGroupBy(TM, t)
	RunOrderBy(TM, t)
	RunPagination(TM, t)
	RunAssociationDiscriminator(TM, t)
	RunAssociationDiscriminatorReverse(TM, t)
	RunTableDiscriminator(TM, t)
	RunJoinTableDiscriminator(TM, t)
	RunVirtualColumns(TM, t)
	RunCustomFunction(TM, t)
	RunRawSQL(TM, t)
	RunHaving(TM, t)
	RunUnion(TM, t)
}

func ResetDB(TM ITransactionManager) {
	if err := TM.Transaction(func(DB IDb) error {
		var err error

		// clear author_books
		if _, err = DB.Delete(AUTHOR_BOOK).Execute(); err != nil {
			return err
		}

		// clear authors
		if _, err = DB.Delete(AUTHOR).Execute(); err != nil {
			return err
		}

		// clear books_i18n
		if _, err = DB.Delete(BOOK_I18N).Execute(); err != nil {
			return err
		}

		// clear books
		if _, err = DB.Delete(BOOK).Execute(); err != nil {
			return err
		}

		// clear publishers
		if _, err = DB.Delete(PUBLISHER).Execute(); err != nil {
			return err
		}

		// insert publisher
		insert := DB.Insert(PUBLISHER).
			Columns(PUBLISHER_C_ID, PUBLISHER_C_VERSION, PUBLISHER_C_NAME).
			Values(1, 1, "Geek Publications")
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		// test UTF8
		insert.Values(2, 1, PUBLISHER_UTF8_NAME)
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		// insert book
		insert = DB.Insert(BOOK).
			Columns(BOOK_C_ID, BOOK_C_VERSION, BOOK_C_NAME, BOOK_C_PRICE, BOOK_C_PUBLISHED, BOOK_C_PUBLISHER_ID).
			Values(1, 1, "Once Upon a Time...", 34.5, time.Date(2012, time.November, 10, 0, 0, 0, 0, time.UTC), 1)
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		insert.Values(2, 1, "Cookbook", 7.2, time.Date(2013, time.July, 24, 0, 0, 0, 0, time.UTC), 2)
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		insert.Values(3, 1, "Scrapbook", 6.5, time.Date(2012, time.April, 01, 0, 0, 0, 0, time.UTC), 2)
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		// insert book_i18n
		insert = DB.Insert(BOOK_I18N).
			Columns(BOOK_I18N_C_ID, BOOK_I18N_C_VERSION, BOOK_I18N_C_BOOK_ID, BOOK_I18N_C_LANG, BOOK_I18N_C_TITLE).
			Values(1, 1, 1, LANG, BOOK_LANG_TITLE)
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		// insert Author
		insert = DB.Insert(AUTHOR).
			Columns(AUTHOR_C_ID, AUTHOR_C_VERSION, AUTHOR_C_NAME).
			Values(1, 1, "John Doe")
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		insert.Values(2, 1, "Jane Doe")
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		insert.Values(3, 1, "Graça Tostão")
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		// insert Author-Book
		// John Doe - Scrapbook
		insert = DB.Insert(AUTHOR_BOOK).
			Columns(AUTHOR_BOOK_C_AUTHOR_ID, AUTHOR_BOOK_C_BOOK_ID).
			Values(1, 3)
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		// Jane Doe - Scrapbook
		insert.Values(2, 3)
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		// Graça Tostão - Once Upon a Time...
		insert.Values(3, 1)
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		// Jane Doe - Cookbook
		insert.Values(1, 2)
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		// Graça Tostão - Cookbook
		insert.Values(3, 2)
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		panic(err)
	}
}

func ResetDB2(TM ITransactionManager) {
	if err := TM.Transaction(func(DB IDb) error {
		var err error

		// clear projects
		if _, err = DB.Delete(PROJECT).Execute(); err != nil {
			return err
		}

		// clear emplyees
		if _, err = DB.Delete(EMPLOYEE).Execute(); err != nil {
			return err
		}

		// clear consultant
		if _, err = DB.Delete(CONSULTANT).Execute(); err != nil {
			return err
		}

		// insert CONSULTANT
		insert := DB.Insert(CONSULTANT).
			Columns(CONSULTANT_C_ID, CONSULTANT_C_VERSION, CONSULTANT_C_NAME).
			Values(1, 1, "John")
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		// insert employee
		insert = DB.Insert(EMPLOYEE).
			Columns(EMPLOYEE_C_ID, EMPLOYEE_C_VERSION, EMPLOYEE_C_NAME).
			Values(1, 1, "Mary")
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		insert.Values(2, 1, "Kate")
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		// insert Project
		insert = DB.Insert(PROJECT).
			Columns(PROJECT_C_ID, PROJECT_C_VERSION, PROJECT_C_NAME, PROJECT_C_MANAGER_ID, PROJECT_C_MANAGER_TYPE, PROJECT_C_STATUS).
			Values(1, 1, "Bridge", 1, "C", "ANA")
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		_, err = insert.Values(2, 1, "Plane", 1, "E", "DEV").Execute()
		if err != nil {
			return err
		}

		_, err = insert.Values(3, 1, "Car", 2, "E", "DEV").Execute()
		if err != nil {
			return err
		}

		_, err = insert.Values(4, 1, "House", 2, "E", "TEST").Execute()
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		panic(err)
	}
}

func ResetDB3(TM ITransactionManager) {
	if err := TM.Transaction(func(DB IDb) error {
		var err error

		// clear projects
		if _, err = DB.Delete(CATALOG).Execute(); err != nil {
			return err
		}

		// insert publisher
		insert := DB.Insert(CATALOG).
			Columns(CATALOG_C_ID, CATALOG_C_VERSION, CATALOG_C_DOMAIN, CATALOG_C_CODE, CATALOG_C_DESCRIPTION)

		_, err = insert.Values(1, 1, "GENDER", "M", "Male").Execute()
		if err != nil {
			return err
		}

		_, err = insert.Values(2, 1, "GENDER", "F", "Female").Execute()
		if err != nil {
			return err
		}

		_, err = insert.Values(3, 1, "STATUS", "ANA", "Analysis").Execute()
		if err != nil {
			return err
		}

		_, err = insert.Values(4, 1, "STATUS", "DEV", "Development").Execute()
		if err != nil {
			return err
		}

		_, err = insert.Values(5, 1, "STATUS", "TEST", "Testing").Execute()
		if err != nil {
			return err
		}

		_, err = insert.Values(6, 1, "STATUS", "PROD", "Production").Execute()
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		panic(err)
	}
}

func RunSelectUTF8(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// get the database context
	store := TM.Store()
	// the target entity
	var publisher = Publisher{}

	ok, err := store.Query(PUBLISHER).
		All().
		Where(PUBLISHER_C_ID.Matches(2)).
		SelectTo(&publisher)

	if err != nil {
		t.Fatalf("%s", err)
	} else if !ok || *publisher.Id != 2 || *publisher.Version != 1 || *publisher.Name != PUBLISHER_UTF8_NAME {
		t.Fatalf("The record for publisher id 2, was not properly retrived. Retrived %s", publisher)
	}
}

func RunInsertReturningKey(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	var err error
	if err = TM.Transaction(func(store IDb) error {
		key, err := store.Insert(PUBLISHER).
			Columns(PUBLISHER_C_ID, PUBLISHER_C_VERSION, PUBLISHER_C_NAME).
			Values(nil, 1, "New Editions").
			Execute()
		if err != nil {
			return err
		}

		if key == 0 {
			t.Fatal("The Auto Insert Key for a null ID column was not retrived")
		}

		logger.Debugf("The Auto Insert Key for a null ID column was %v", key)

		key = 0
		// now without declaring the ID column
		key, err = store.Insert(PUBLISHER).
			Columns(PUBLISHER_C_VERSION, PUBLISHER_C_NAME).
			Values(1, "Second Editions").
			Execute()
		if err != nil {
			return err
		}

		if key == 0 {
			t.Fatal("The Auto Insert Key for a absent ID column was not retrived")
		}

		logger.Debugf("The Auto Insert Key for a absent ID column was %v", key)

		return nil
	}); err != nil {
		t.Fatalf("Failed Insert Returning Key: %s", err)
	}
}

func RunInsertStructReturningKey(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	var err error
	if err = TM.Transaction(func(store IDb) error {
		var pub Publisher
		pub.Name = ext.StrPtr("Untited Editors")
		key, err := store.Insert(PUBLISHER).Submit(pub)
		if err != nil {
			return err
		}

		if key == 0 {
			t.Fatal("The Auto Insert Key for the ID column was not retrived")
		}

		var pubPtr = new(Publisher)
		pubPtr.Name = ext.StrPtr("Untited Editors")
		key, err = store.Insert(PUBLISHER).Submit(pubPtr)
		if err != nil {
			return err
		}

		if key == 0 {
			t.Fatal("The Auto Insert Key for the ID column was not retrived")
		}

		return nil
	}); err != nil {
		t.Fatalf("Failed Struct Insert Return Key: %s", err)
	}
}

func RunSimpleUpdate(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	var err error
	if err = TM.Transaction(func(store IDb) error {
		affectedRows, err := store.Update(PUBLISHER).
			Set(PUBLISHER_C_NAME, "Untited Editors"). // column to update
			Set(PUBLISHER_C_VERSION, 2).              // increment version
			Where(
			PUBLISHER_C_ID.Matches(1),
			PUBLISHER_C_VERSION.Matches(1),
		).
			Execute()
		if err != nil {
			return err
		}

		if affectedRows != 1 {
			t.Fatal("The record was not updated")
		}

		return nil
	}); err != nil {
		t.Fatalf("Failed Update Test: %s", err)
	}
}

func RunStructUpdate(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	var err error
	if err = TM.Transaction(func(store IDb) error {
		var publisher Publisher
		publisher.Name = ext.StrPtr("Untited Editors")
		publisher.Id = ext.Int64Ptr(1)
		publisher.Version = ext.Int64Ptr(1)
		affectedRows, err := store.Update(PUBLISHER).Submit(publisher)
		if err != nil {
			return err
		}

		if affectedRows != 1 {
			t.Fatal("The record was not updated")
		}

		return nil
	}); err != nil {
		t.Fatalf("Failed Update Test: %s", err)
	}
}

func RunUpdateSubquery(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	if err := TM.Transaction(func(store IDb) error {
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
		if err != nil {
			return err
		}

		if affectedRows != 1 {
			t.Fatal("The record was not updated")
		}

		return nil

	}); err != nil {
		t.Fatalf("Failed Update with Subquery Test: %s", err)
	}
}

func RunSimpleDelete(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	if err := TM.Transaction(func(store IDb) error {
		// clears any relation with book id = 2
		store.Delete(AUTHOR_BOOK).Where(AUTHOR_BOOK_C_BOOK_ID.Matches(2)).Execute()

		affectedRows, err := store.Delete(BOOK).Where(BOOK_C_ID.Matches(2)).Execute()
		if err != nil {
			return err
		}
		if affectedRows != 1 {
			t.Fatal("The record was not deleted")
		}

		return nil
	}); err != nil {
		t.Fatalf("Failed TestSimpleDelete: %s", err)
	}
}

func RunStructDelete(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	if err := TM.Transaction(func(store IDb) error {
		// clears any relation with book id = 1
		store.Delete(AUTHOR_BOOK).Where(AUTHOR_BOOK_C_BOOK_ID.Matches(2)).Execute()

		var book Book
		book.Id = ext.Int64Ptr(2)
		book.Version = ext.Int64Ptr(1)
		affectedRows, err := store.Delete(BOOK).Submit(book)
		if err != nil {
			return err
		}
		if affectedRows != 1 {
			t.Fatal("The record was not deleted")
		}

		return nil
	}); err != nil {
		t.Fatalf("Failed ... Test: %s", err)
	}
}

func RunSelectInto(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	var name string
	ok, err := store.Query(PUBLISHER).
		Column(PUBLISHER_C_NAME).
		Where(PUBLISHER_C_ID.Matches(2)).
		SelectInto(&name)
	if err != nil {
		t.Fatalf("%s", err)
	} else if !ok || name != PUBLISHER_UTF8_NAME {
		t.Fatalf("Failed SelectInto. The name for publisher id 2, was not properly retrived. Retrived %s", name)
	}
}

func RunSelectTreeTo(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	var publisher Publisher
	ok, err := store.Query(PUBLISHER).
		All().
		Outer(PUBLISHER_A_BOOKS).
		Fetch(). // add all columns off book in the query
		Where(PUBLISHER_C_ID.Matches(2)).
		SelectTreeTo(&publisher, true)
	if err != nil {
		t.Fatalf("%s", err)
	} else if !ok || publisher.Id == nil {
		t.Fatal("The record for publisher id 2, was not retrived")
	} else {
		// check list size of books
		if len(publisher.Books) != 2 {
			t.Fatalf("The list of books for the publisher with id 2 was incorrectly retrived. Expected 2 got %v", len(publisher.Books))
		}
	}
}

func RunSelectTree(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	result, err := store.Query(PUBLISHER).
		All().
		Outer(PUBLISHER_A_BOOKS).
		Fetch(). // add all columns off book in the query
		Where(PUBLISHER_C_ID.Matches(2)).
		SelectTree((*Publisher)(nil), true)
	if err != nil {
		t.Fatalf("%s", err)
	} else if result == nil {
		t.Fatal("The record for publisher id 2, was not retrived")
	} else {
		publisher := result.(*Publisher)
		// check list size of books
		if len(publisher.Books) != 2 {
			t.Fatalf("The list of books for the publisher with id 2 was incorrectly retrived. Expected 2 got %v", len(publisher.Books))
		}
	}
}

func RunListFor(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	books := make([]*Book, 0) // mandatory use pointers
	err := store.Query(BOOK).
		All().
		ListFor(func(book *Book) {
		books = append(books, book)
	})
	if err != nil {
		t.Fatalf("Failed ListFor Test: %s", err)
	}

	if len(books) != 3 {
		t.Fatalf("Expected 3 returned books, but got %v", len(books))
	} else {
		for _, v := range books {
			if v.Id == nil {
				t.Fatalf("A book has invalid Id and therefore was not retrived")
			}
		}
	}
}

func RunListOf(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	books, err := store.Query(BOOK).
		All().
		ListOf((*Book)(nil))
	if err != nil {
		t.Fatalf("Failed ListOf Test: %s", err)
	}

	if books.Size() != 3 {
		t.Fatalf("Expected 3 returned books, but got %v", books.Size())
	} else {
		for e := books.Enumerator(); e.HasNext(); {
			book := e.Next().(*Book)
			if book.Id == nil {
				t.Fatalf("A book has invalid Id and therefore was not retrived")
			}
		}
	}
}

func RunListFlatTreeFor(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	publishers := make([]*Publisher, 0)
	err := store.Query(PUBLISHER).
		All().
		Outer(PUBLISHER_A_BOOKS).
		Fetch(). // add all columns off book in the query
		Where(PUBLISHER_C_ID.Matches(2)).
		ListFlatTreeFor(func(publisher *Publisher) {
		publishers = append(publishers, publisher)
	})
	if err != nil {
		t.Fatalf("%s", err)
	} else if len(publishers) != 2 {
		t.Fatalf("The record for publisher id 2, was not retrived. Expected collection size of 2, got %v", len(publishers))
	} else {
		for _, publisher := range publishers {
			// check list size of books
			if publisher.Id == nil {
				t.Fatalf("A book has invalid Id and therefore was not retrived")
			}
			if len(publisher.Books) != 1 {
				t.Fatalf("The list of books for the publisher with id 2 was incorrectly retrived. Expected 1 got %v", len(publisher.Books))
			}
		}
	}
}

func RunListTreeOf(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	publishers, err := store.Query(PUBLISHER).
		All().
		Outer(PUBLISHER_A_BOOKS).
		Fetch(). // add all columns off book in the query
		Where(PUBLISHER_C_ID.Matches(2)).
		ListTreeOf((*Publisher)(nil))
	if err != nil {
		t.Fatalf("%s", err)
	} else if publishers.Size() != 1 {
		t.Fatalf("The record for publisher id 2, was not retrived. Expected collection size of 1, got %v", publishers.Size())
	} else {
		for e := publishers.Enumerator(); e.HasNext(); {
			publisher := e.Next().(*Publisher)
			// check list size of books
			if publisher.Id == nil {
				t.Fatalf("A book has invalid Id and therefore was not retrived")
			}
			if len(publisher.Books) != 2 {
				t.Fatalf("The list of books for the publisher with id 2 was incorrectly retrived. Expected 2 got %v", len(publisher.Books))
			}
		}
	}
}

func RunListSimpleFor(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	names := make([]string, 0)
	var name string
	err := store.Query(PUBLISHER).
		Column(PUBLISHER_C_NAME).
		ListSimpleFor(func() {
		names = append(names, name)
	}, &name)
	if err != nil {
		t.Fatalf("Failed TestListSimpleFor: %s", err)
	}

	if len(names) != 2 {
		t.Fatalf("Expected 2 Publisher names, but got %v", len(names))
	}
}

func RunColumnSubquery(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	subquery := store.Query(BOOK).Alias("b").
		Column(Sum(BOOK_C_PRICE)).
		Where(
		BOOK_C_PUBLISHER_ID.Matches(Col(PUBLISHER_C_ID).For("p")),
	)

	var dtos = make([]*Dto, 0)
	err := store.Query(PUBLISHER).Alias("p").
		Column(PUBLISHER_C_NAME).
		Column(subquery).As("Value").
		ListFor(func(dto *Dto) {
		dtos = append(dtos, dto)
	})

	if err != nil {
		t.Fatalf("Failed TestColumnSubquery: %s", err)
	}

	if len(dtos) != 2 {
		t.Fatalf("Expected 2 Publisher names, but got %v", len(dtos))
	}

	for k, v := range dtos {
		logger.Debugf("dtos[%v] = %+v", k, *v)
	}
}

func RunWhereSubquery(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	subquery := store.Query(BOOK).
		Distinct().
		Column(BOOK_C_PUBLISHER_ID).
		Where(
		BOOK_C_PRICE.LesserOrMatch(10),
	)

	var dtos = make([]*Dto, 0)
	err := store.Query(PUBLISHER).
		Column(PUBLISHER_C_NAME).
		Inner(PUBLISHER_A_BOOKS).
		Include(BOOK_C_NAME).As("OtherName").
		Include(BOOK_C_PRICE).As("Value").
		Join().
		Where(PUBLISHER_C_ID.In(subquery)).
		ListFor(func(dto *Dto) {
		dtos = append(dtos, dto)
	})

	if err != nil {
		t.Fatalf("Failed TestWhereSubquery: %s", err)
	}

	if len(dtos) != 2 {
		t.Fatalf("Expected 2 Publisher names, but got %v", len(dtos))
	}

	for k, v := range dtos {
		logger.Debugf("dtos[%v] = %+v", k, *v)
	}
}

func RunInnerOn(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// gets all publishers that had a book published before 2013
	store := TM.Store()
	var publishers = make([]*Publisher, 0)
	err := store.Query(PUBLISHER).
		All().
		Distinct().
		Inner(PUBLISHER_A_BOOKS).
		On(BOOK_C_PUBLISHED.Lesser(time.Date(2013, time.January, 1, 0, 0, 0, 0, time.UTC))).
		Join().
		ListFor(func(publisher *Publisher) {
		publishers = append(publishers, publisher)
	})

	if err != nil {
		t.Fatalf("Failed TestInnerOn: %s", err)
	}

	if len(publishers) != 2 {
		t.Fatalf("Expected 2 Publishers, but got %v", len(publishers))
	}

	for k, v := range publishers {
		logger.Debugf("publishers[%v] = %s", k, *v)
		if v.Id == nil {
			t.Fatal("Expected a valid Id, but got nil")
		}
	}
}

func RunInnerOn2(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	var publishers = make([]*Publisher, 0)
	err := store.Query(PUBLISHER).
		All().
		Distinct().
		Inner(PUBLISHER_A_BOOKS).
		On(BOOK_C_PUBLISHED.Lesser(time.Date(2013, time.January, 1, 0, 0, 0, 0, time.UTC))).
		Inner(BOOK_A_AUTHORS).
		On(AUTHOR_C_NAME.Like("%Doe")).
		Join().
		ListFor(func(publisher *Publisher) {
		publishers = append(publishers, publisher)
	})

	if err != nil {
		t.Fatalf("Failed TestInnerOn: %s", err)
	}

	if len(publishers) != 1 {
		t.Fatalf("Expected 1 Publishers, but got %v", len(publishers))
	}

	for k, v := range publishers {
		logger.Debugf("publishers[%v] = %s", k, *v)
		if v.Id == nil {
			t.Fatal("Expected a valid Id, but got nil")
		}
	}
}

func RunOuterFetch(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	result, err := store.Query(PUBLISHER).
		All().
		Outer(PUBLISHER_A_BOOKS, BOOK_A_AUTHORS).
		Fetch().
		ListTreeOf((*Publisher)(nil))

	if err != nil {
		t.Fatalf("Failed TestOuterFetch: %s", err)
	}

	publishers := result.AsSlice().([]*Publisher)

	if len(publishers) != 2 {
		t.Fatalf("Expected 2 Publishers, but got %v", len(publishers))
	}

	pub := publishers[0]
	if len(pub.Books) != 1 {
		t.Fatalf("Expected 1 Book for Publishers %s, but got %v", pub.Name, len(pub.Books))
	}

	book := pub.Books[0]
	if len(book.Authors) != 1 {
		t.Fatalf("Expected 1 Author for Book %s, but got %v", book.Name, len(book.Authors))
	}

	pub = publishers[1]
	if len(pub.Books) != 2 {
		t.Fatalf("Expected 2 Book for Publishers %s, but got %v", pub.Name, len(pub.Books))
	}

	book = pub.Books[0]
	if len(book.Authors) != 2 {
		t.Fatalf("Expected 2 Author for Book %s, but got %v", book.Name, len(book.Authors))
	}

	book = pub.Books[1]
	if len(book.Authors) != 2 {
		t.Fatalf("Expected 2 Author for Book %s, but got %v", book.Name, len(book.Authors))
	}

	for k, v := range publishers {
		logger.Debugf("publishers[%v] = %s", k, v.String())
		if v.Id == nil {
			t.Fatal("Expected a valid Id, but got nil")
		}
	}
}

func RunGroupBy(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	var dtos = make([]*Dto, 0)
	err := store.Query(PUBLISHER).
		Column(PUBLISHER_C_NAME).
		Outer(PUBLISHER_A_BOOKS).
		Include(Sum(BOOK_C_PRICE)).As("Value").
		Join().
		GroupByPos(1).
		ListFor(func(dto *Dto) {
		dtos = append(dtos, dto)
	})

	if err != nil {
		t.Fatalf("Failed TestGroupBy: %s", err)
	}

	if len(dtos) != 2 {
		t.Fatalf("Expected 2 Publisher names, but got %v", len(dtos))
	}

	for k, v := range dtos {
		logger.Debugf("dtos[%v] = %+v", k, *v)
	}
}

func RunOrderBy(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	var publishers = make([]*Publisher, 0)
	err := store.Query(PUBLISHER).
		All().
		OrderBy(PUBLISHER_C_NAME).
		Asc(true).
		ListFor(func(publisher *Publisher) {
		publishers = append(publishers, publisher)
	})

	if err != nil {
		t.Fatalf("Failed TestGroupBy: %s", err)
	}

	if len(publishers) != 2 {
		t.Fatalf("Expected 2 Publisher names, but got %v", len(publishers))
	}

	for k, v := range publishers {
		logger.Debugf("publishers[%v] = %s", k, v.String())
		if v.Id == nil {
			t.Fatal("Expected a valid Id, but got nil")
		}
	}
}

func RunPagination(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	var publishers = make([]*Publisher, 0)
	err := store.Query(PUBLISHER).
		All().
		Outer(PUBLISHER_A_BOOKS, BOOK_A_AUTHORS).
		Fetch().
		Order(PUBLISHER_C_NAME).Asc(true).
		Skip(2).  // skip the first 2 records
		Limit(3). // returns next 3 records
		ListFlatTreeFor(func(publisher *Publisher) {
		publishers = append(publishers, publisher)
	})

	if err != nil {
		t.Fatalf("Failed TestPagination: %s", err)
	}

	if len(publishers) != 3 {
		t.Fatalf("Expected 3 Publisher names, but got %v", len(publishers))
	}

	for k, v := range publishers {
		logger.Debugf("publishers[%v] = %s", k, v.String())
		if v.Id == nil {
			t.Fatal("Expected a valid Id, but got nil")
		}
	}
}

func RunAssociationDiscriminator(TM ITransactionManager, t *testing.T) {
	ResetDB2(TM)

	store := TM.Store()
	result, err := store.Query(PROJECT).
		All().
		Inner(PROJECT_A_EMPLOYEE).
		Fetch().
		Order(PROJECT_C_NAME).Asc(true).
		ListTreeOf((*Project)(nil))

	if err != nil {
		t.Fatalf("Failed TestAssociationDiscriminator: %s", err)
	}

	projects := result.AsSlice().([]*Project)

	if len(projects) != 3 {
		t.Fatalf("Expected 3 Projects, but got %v", len(projects))
	}

	for _, v := range projects {
		if len(v.Employee) == 0 {
			t.Fatalf("Expected Employee for project %s but got <nil>", v.Name)
		}
	}

	for k, v := range projects {
		logger.Debugf("Projects[%v] = %s", k, v.String())
		if v.Id == nil {
			t.Fatal("Expected a valid Id, but got nil")
		}
	}
}

func RunAssociationDiscriminatorReverse(TM ITransactionManager, t *testing.T) {
	ResetDB2(TM)

	store := TM.Store()
	result, err := store.Query(EMPLOYEE).
		All().
		Inner(EMPLOYEE_A_PROJECT).
		Fetch().
		Order(EMPLOYEE_C_NAME).Asc(true).
		ListTreeOf((*Employee)(nil))

	if err != nil {
		t.Fatalf("Failed TestAssociationDiscriminatorReverse: %s", err)
	} else {

		employees := result.AsSlice().([]*Employee)

		if len(employees) != 2 {
			t.Fatalf("Expected 2 Employees, but got %v", len(employees))
		}

		for _, v := range employees {
			if v.Project == nil {
				t.Fatalf("Expected Project for project '%v' but got <nil>", *v.Name)
			}
		}

		for k, v := range employees {
			logger.Debugf("Employees[%v] = %s", k, v.String())
			if v.Id == nil {
				t.Fatal("Expected a valid Id, but got nil")
			}
		}
	}

}

func RunTableDiscriminator(TM ITransactionManager, t *testing.T) {
	ResetDB3(TM)

	store := TM.Store()
	statuses := make([]*Status, 0)
	err := store.Query(STATUS).
		All().
		ListFor(func(status *Status) {
		statuses = append(statuses, status)
	})

	if err != nil {
		t.Fatalf("Failed Query in TestTableDiscriminator: %s", err)
	}

	if len(statuses) != 4 {
		t.Fatalf("Expected 4 Statuses, but got %v", len(statuses))
	}

	for k, v := range statuses {
		logger.Debugf("Statuss[%v] = %s", k, v.String())
		if v.Id == nil {
			t.Fatal("Expected a valid Id, but got nil")
		}
	}

	var tmp int64
	status := statuses[0]
	status.Code = ext.StrPtr("X")
	status.Description = ext.StrPtr("Unknown")
	tmp, err = store.Update(STATUS).Submit(status)
	if err != nil {
		t.Fatalf("Failed Update in TestTableDiscriminator: %s", err)
	}
	if tmp != 1 {
		t.Fatalf("Expected 1 rows updates, but got %v", tmp)
	}

	tmp, err = store.Delete(STATUS).Execute()
	if err != nil {
		t.Fatalf("Failed Delete in TestTableDiscriminator: %s", err)
	}
	if tmp != 4 {
		t.Fatalf("Expected 4 rows deleted, but got %v", tmp)
	}

	status = new(Status)
	status.Code = ext.StrPtr("X")
	status.Description = ext.StrPtr("Unknown")
	tmp, err = store.Insert(STATUS).Submit(status)
	if err != nil {
		t.Fatalf("Failed Insert in TestTableDiscriminator: %s", err)
	}
	if tmp == 0 {
		t.Fatal("Expected Id different of 0")
	}

}

func RunJoinTableDiscriminator(TM ITransactionManager, t *testing.T) {
	ResetDB3(TM)

	store := TM.Store()
	result := make([]*Project, 0)
	err := store.Query(PROJECT).
		All().
		Outer(PROJECT_A_STATUS).
		Fetch().
		ListFlatTreeFor(func(project *Project) {
		result = append(result, project)
	})

	if err != nil {
		t.Fatalf("Failed TestJoinTableDiscriminator: %s", err)
	}

	if len(result) != 4 {
		t.Fatalf("Expected 4 Projects, but got %v", len(result))
	}

	for k, v := range result {
		logger.Debugf("Projects[%v] = %s", k, v.String())
		if v.Id == nil {
			t.Fatal("Expected a valid Id, but got nil")
		}
	}
}

func RunVirtualColumns(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// get the database context
	store := TM.Store()
	// the target entity
	var book = Book{}
	ok, err := store.Query(BOOK).
		All().
		Where(BOOK_C_ID.Matches(1)).
		SelectTo(&book)
	if err != nil {
		t.Fatalf("Failed TestVirtualColumns: %s", err)
	} else if !ok || *book.Id != 1 || *book.Version != 1 || *book.Title != BOOK_LANG_TITLE {
		t.Fatalf("The record for book with id 1, was not properly retrived. Retrived %s", (&book).String())
	}
}

func RunCustomFunction(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// get the database context
	store := TM.Store()
	books := make([]*Book, 0)
	err := store.Query(BOOK).
		All().
		Where(
		SecondsDiff(
			time.Date(2013, time.July, 24, 0, 0, 0, 0, time.UTC),
			BOOK_C_PUBLISHED,
		).
			Greater(1000),
	).
		ListFor(func(book *Book) {
		books = append(books, book)
	})

	if err != nil {
		t.Fatalf("Failed TestCustomFunction: %s", err)
	}

	if len(books) != 2 {
		t.Fatalf("Expected 2 Books, but got %v", len(books))
	}

	for k, v := range books {
		logger.Debugf("books[%v] = %s", k, v.String())
		if v.Id == nil {
			t.Fatal("Expected a valid Id, but got nil")
		}
	}
}

func RunRawSQL(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// get the database connection
	dba := dbx.NewSimpleDBA(TM.Store().GetConnection())
	result := make([]string, 0)
	err := dba.QueryClosure(RAW_SQL, func(rows *sql.Rows) error {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		result = append(result, name)
		return nil
	}, "%book")

	if err != nil {
		t.Fatalf("Failed TestRawSQL: %s", err)
	}

	for k, v := range result {
		logger.Debugf("books[%v] = %s", k, v)
		if v == "" {
			t.Fatal("Expected a valid Name, but got empty")
		}
	}
}

func RunHaving(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// get the database context
	store := TM.Store()
	sales := make([]*PublisherSales, 0)
	err := store.Query(PUBLISHER).
		Column(PUBLISHER_C_NAME).
		Outer(PUBLISHER_A_BOOKS).
		Include(Sum(BOOK_C_PRICE)).As("ThisYear").
		Join().
		GroupByPos(1).
		Having(Alias("ThisYear").Greater(30)).
		ListFor(func(sale *PublisherSales) {
		sales = append(sales, sale)
	})

	if err != nil {
		t.Fatalf("Failed TestHaving: %s", err)
	}

	if len(sales) != 1 {
		t.Fatalf("Expected 1 PublisherSales, but got %v", len(sales))
	}

	for k, v := range sales {
		logger.Debugf("sales[%v] = %s", k, v.String())
	}
}

func RunUnion(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// get the database context
	store := TM.Store()
	sales := make([]*PublisherSales, 0)
	err := store.Query(PUBLISHER).
		Column(PUBLISHER_C_ID).
		Column(PUBLISHER_C_NAME).
		Outer(PUBLISHER_A_BOOKS).
		Include(Sum(Coalesce(BOOK_C_PRICE, 0))).As("ThisYear").
		On(
		Range(
			BOOK_C_PUBLISHED,
			time.Date(2013, time.January, 01, 0, 0, 0, 0, time.UTC),
			time.Date(2013, time.December, 31, 23, 59, 59, 1e9-1, time.UTC),
		),
	).
		Join().
		Column(AsIs(0)).As("PreviousYear").
		GroupByPos(1, 2).
		UnionAll(
		store.Query(PUBLISHER).Alias("u").
			Column(PUBLISHER_C_ID).
			Column(PUBLISHER_C_NAME).
			Outer(PUBLISHER_A_BOOKS).
			Column(AsIs(0)).As("ThisYear").
			Include(Sum(Coalesce(BOOK_C_PRICE, 0))).As("PreviousYear").
			On(
			Range(
				BOOK_C_PUBLISHED,
				time.Date(2012, time.January, 01, 0, 0, 0, 0, time.UTC),
				time.Date(2012, time.December, 31, 23, 59, 59, 1e9-1, time.UTC),
			),
		).
			Join().
			GroupByPos(1, 2),
	).
		ListFor(func(sale *PublisherSales) {
		logger.Debugf("sales = %s", sale.String())
		found := false
		for _, v := range sales {
			if sale.Id == v.Id {
				v.ThisYear += sale.ThisYear
				v.PreviousYear += sale.PreviousYear
				found = true
				break
			}
		}
		if !found {
			sales = append(sales, sale)
		}
	})

	if err != nil {
		t.Fatalf("Failed TestUnion: %s", err)
	}

	for k, v := range sales {
		logger.Debugf("sales[%v] = %s", k, v.String())
	}

}
