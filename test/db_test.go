package test

import (
	. "github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/dbx"
	trx "github.com/quintans/goSQL/translators"
	"github.com/quintans/toolkit/ext"
	"github.com/quintans/toolkit/log"

	_ "github.com/go-sql-driver/mysql"

	"database/sql"
	"testing"
	"time"
)

var TM ITransactionManager

var logger = log.LoggerFor("github.com/quintans/goSQL/test")

func init() {
	log.Register("/", log.DEBUG, log.NewConsoleAppender(false))

	/*
	 * =======================
	 * BEGIN DATABASE CONFIG
	 * =======================
	 */
	// database configuration

	mydb, err := sql.Open("mysql", "root:root@/ezsql?parseTime=true")
	if err != nil {
		panic(err)
	}

	// wake up the database pool
	err = mydb.Ping()
	if err != nil {
		panic(err)
	}

	TM = NewTransactionManager(
		// database
		mydb,
		// databse context factory
		func(c dbx.IConnection) IDb {
			//return db.NewDb(c, trx.NewFirebirdSQLTranslator())
			return NewDb(c, trx.NewMySQL5Translator())
		},
		// statement cache
		1000,
	)
	/*
	 * =======================
	 * END DATABASE CONFIG
	 * =======================
	 */
}

const (
	PUBLISHER_UTF8_NAME = "Edições Lusas"
)

func ResetDB() {
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
			Values(1, 1, "Once Upon a Time...", 34.5, time.Date(2009, time.November, 10, 0, 0, 0, 0, time.UTC), 1)
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

func ResetDB2() {
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

func ResetDB3() {
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

func TestSelectUTF8(t *testing.T) {
	ResetDB()

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

func TestInsertReturningKey(t *testing.T) {
	ResetDB()

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

		return nil
	}); err != nil {
		t.Fatalf("Failed Insert Returning Key: %s", err)
	}
}

func TestInsertStructReturningKey(t *testing.T) {
	ResetDB()

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

func TestSimpleUpdate(t *testing.T) {
	ResetDB()

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

func TestStructUpdate(t *testing.T) {
	ResetDB()

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

func TestUpdateSubquery(t *testing.T) {
	ResetDB()

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

func TestSimpleDelete(t *testing.T) {
	ResetDB()

	if err := TM.Transaction(func(store IDb) error {
		// clears any relation with book id = 1
		store.Delete(AUTHOR_BOOK).Where(AUTHOR_BOOK_C_BOOK_ID.Matches(1)).Execute()

		affectedRows, err := store.Delete(BOOK).Where(BOOK_C_ID.Matches(1)).Execute()
		if err != nil {
			return err
		}
		if affectedRows != 1 {
			t.Fatal("The record was not updated")
		}

		return nil
	}); err != nil {
		t.Fatalf("Failed ... Test: %s", err)
	}
}

func TestStructDelete(t *testing.T) {
	ResetDB()

	if err := TM.Transaction(func(store IDb) error {
		// clears any relation with book id = 1
		store.Delete(AUTHOR_BOOK).Where(AUTHOR_BOOK_C_BOOK_ID.Matches(1)).Execute()

		var book Book
		book.Id = ext.Int64Ptr(1)
		book.Version = ext.Int64Ptr(1)
		affectedRows, err := store.Delete(BOOK).Submit(book)
		if err != nil {
			return err
		}
		if affectedRows != 1 {
			t.Fatal("The record was not updated")
		}

		return nil
	}); err != nil {
		t.Fatalf("Failed ... Test: %s", err)
	}
}

func TestSelectInto(t *testing.T) {
	ResetDB()

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

func TestSelectTreeTo(t *testing.T) {
	ResetDB()

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

func TestSelectTree(t *testing.T) {
	ResetDB()

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

func TestListFor(t *testing.T) {
	ResetDB()

	store := TM.Store()
	books := make([]*Book, 0) // mandatory use pointers
	err := store.Query(BOOK).
		All().
		ListFor(func() interface{} {
		book := new(Book)
		books = append(books, book)
		return book
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

func TestListOf(t *testing.T) {
	ResetDB()

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

func TestListFlatTreeFor(t *testing.T) {
	ResetDB()

	store := TM.Store()
	publishers := make([]*Publisher, 0)
	err := store.Query(PUBLISHER).
		All().
		Outer(PUBLISHER_A_BOOKS).
		Fetch(). // add all columns off book in the query
		Where(PUBLISHER_C_ID.Matches(2)).
		ListFlatTreeFor(func() interface{} {
		publisher := new(Publisher)
		publishers = append(publishers, publisher)
		return publisher
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

func TestListTreeOf(t *testing.T) {
	ResetDB()

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

func TestListSimpleFor(t *testing.T) {
	ResetDB()

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

func TestColumnSubquery(t *testing.T) {
	ResetDB()

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
		ListFor(func() interface{} {
		dto := new(Dto)
		dtos = append(dtos, dto)
		return dto
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

func TestWhereSubquery(t *testing.T) {
	ResetDB()

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
		ListFor(func() interface{} {
		dto := new(Dto)
		dtos = append(dtos, dto)
		return dto
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

func TestInnerOn(t *testing.T) {
	ResetDB()

	// gets all publishers that had a book published before 2013
	store := TM.Store()
	var publishers = make([]*Publisher, 0)
	err := store.Query(PUBLISHER).
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

/*
Query query = db.createQuery(TPainting.T_PAINTING)
		.innerJoin(TPainting.A_ARTIST).on(TArtist.C_ID.is(1L))
		.innerJoin(TPainting.A_GALLERIES).on(TGallery.C_NAME.ilk("%AZUL"));
*/

func TestInnerOn2(t *testing.T) {
	ResetDB()

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
		ListFor(func() interface{} {
		publisher := new(Publisher)
		publishers = append(publishers, publisher)
		return publisher
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

func TestOuterFetch(t *testing.T) {
	ResetDB()

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

func TestGroupBy(t *testing.T) {
	ResetDB()

	store := TM.Store()
	var dtos = make([]*Dto, 0)
	err := store.Query(PUBLISHER).
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

func TestOrderBy(t *testing.T) {
	ResetDB()

	store := TM.Store()
	var publishers = make([]*Publisher, 0)
	err := store.Query(PUBLISHER).
		All().
		OrderBy(PUBLISHER_C_NAME).
		Asc(true).
		ListFor(func() interface{} {
		publisher := new(Publisher)
		publishers = append(publishers, publisher)
		return publisher
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

func TestPagination(t *testing.T) {
	ResetDB()

	store := TM.Store()
	var publishers = make([]*Publisher, 0)
	err := store.Query(PUBLISHER).
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

func TestAssociationDiscriminator(t *testing.T) {
	ResetDB2()

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

func TestAssociationDiscriminatorReverse(t *testing.T) {
	ResetDB2()

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

func TestTableDiscriminator(t *testing.T) {
	ResetDB3()

	store := TM.Store()
	statuses := make([]*Status, 0)
	err := store.Query(STATUS).
		All().
		ListFor(func() interface{} {
		status := new(Status)
		statuses = append(statuses, status)
		return status
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

func TestJoinTableDiscriminator(t *testing.T) {
	ResetDB3()

	store := TM.Store()
	result := make([]*Project, 0)
	err := store.Query(PROJECT).
		All().
		Outer(PROJECT_A_STATUS).
		Fetch().
		ListFlatTreeFor(func() interface{} {
		project := new(Project)
		result = append(result, project)
		return project
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
