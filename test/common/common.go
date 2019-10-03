package common

import (
	"io/ioutil"
	"strings"

	. "github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/dbx"
	"github.com/quintans/toolkit/ext"
	"github.com/quintans/toolkit/log"

	"database/sql"
	"testing"
	"time"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/test")

// custom Db - for setting default parameters
func NewMyDb(connection dbx.IConnection, translator Translator, lang string) *MyDb {
	baseDb := NewDb(connection, translator)
	return &MyDb{baseDb, lang}
}

type MyDb struct {
	*Db
	Lang string
}

func (this *MyDb) Query(table *Table) *Query {
	query := this.Overrider.Query(table)
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
	log.Register("/", log.DEBUG, log.NewConsoleAppender(false), log.NewFileAppender("db_test.log", 0, true, true))
}

var RAW_SQL string

func InitDB(t *testing.T, driverName, dataSourceName string, translator Translator, initSqlFile string) (ITransactionManager, *sql.DB) {
	mydb, err := Connect(driverName, dataSourceName)
	if err != nil {
		t.Fatal(err)
	}

	CreateTables(t, mydb, initSqlFile)

	return NewTransactionManager(
		// database
		mydb,
		// databse context factory
		func(c dbx.IConnection) IDb {
			return NewMyDb(c, translator, "pt")
		},
		// statement cache
		1000,
	), mydb
}

func Connect(driverName, dataSourceName string) (*sql.DB, error) {
	mydb, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	// wake up the database pool
	err = mydb.Ping()
	if err != nil {
		return nil, err
	}
	return mydb, nil
}

func CreateTables(t *testing.T, db *sql.DB, initSqlFile string) {
	logger.Infof("******* Creating tables *******\n")

	sql, err := ioutil.ReadFile(initSqlFile)
	if err != nil {
		t.Fatal(err)
	}

	stmts := strings.Split(string(sql), ";\n")

	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		logger.Debug(stmt)
		if stmt != "" {
			_, err := db.Exec(stmt)
			if err != nil {
				t.Fatalf("sql: %s\n%s", stmt, err)
			}
		}
	}
}

const (
	PUBLISHER_UTF8_NAME = "Edições Lusas"
	AUTHOR_UTF8_NAME    = "Graça Tostão"
	LANG                = "pt"
	BOOK_LANG_TITLE     = "Era uma vez..."

	Firebird = "Firebird"
	Oracle   = "Oracle"
	MySQL    = "MySQL"
	Postgres = "Postgres"
)

type Tester struct {
	DbName string
}

func (tt Tester) RunAll(TM ITransactionManager, t *testing.T) {
	tt.RunDriverConverter(TM, t)
	tt.RunSelectUTF8(TM, t)
	tt.RunRetrive(TM, t)
	tt.RunFindFirst(TM, t)
	tt.RunFindAll(TM, t)
	tt.RunOmitField(TM, t)
	tt.RunModifyField(TM, t)
	tt.RunRemoveAll(TM, t)
	tt.RunInsertReturningKey(TM, t)
	tt.RunInsertStructReturningKey(TM, t)
	tt.RunSimpleUpdate(TM, t)
	tt.RunStructUpdate(TM, t)
	tt.RunStructSaveAndRetrive(TM, t)
	tt.RunUpdateSubquery(TM, t)
	tt.RunSimpleDelete(TM, t)
	tt.RunStructDelete(TM, t)
	tt.RunSelectInto(TM, t)
	tt.RunSelectTree(TM, t)
	tt.RunSelectTreeTwoBranches(TM, t)
	tt.RunSelectFlatTree(TM, t)
	tt.RunListInto(TM, t)
	tt.RunListOf(TM, t)
	tt.RunListFlatTree(TM, t)
	tt.RunListTreeOf(TM, t)
	tt.RunListForSlice(TM, t)
	tt.RunListSimple(TM, t)
	tt.RunSimpleCase(TM, t)
	tt.RunSearchedCase(TM, t)
	tt.RunColumnSubquery(TM, t)
	tt.RunWhereSubquery(TM, t)
	tt.RunInnerOn(TM, t)
	tt.RunInnerOn2(TM, t)
	tt.RunOuterFetchOrder(TM, t)
	tt.RunOuterFetchOrderAs(TM, t)
	tt.RunGroupBy(TM, t)
	tt.RunOrderBy(TM, t)
	tt.RunPagination(TM, t)
	tt.RunAssociationDiscriminator(TM, t)
	tt.RunAssociationDiscriminatorReverse(TM, t)
	tt.RunTableDiscriminator(TM, t)
	tt.RunJoinTableDiscriminator(TM, t)
	tt.RunCustomFunction(TM, t)
	tt.RunRawSQL1(TM, t)
	tt.RunRawSQL2(TM, t)
	tt.RunHaving(TM, t)
	tt.RunUnion(TM, t)
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

		// clear books_bin
		if _, err = DB.Delete(BOOK_BIN).Execute(); err != nil {
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

		insert.Values(2, 1, "Cookbook", 12.5, time.Date(2013, time.July, 24, 0, 0, 0, 0, time.UTC), 2)
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		insert.Values(3, 1, "Scrapbook", 6.5, time.Date(2012, time.April, 01, 0, 0, 0, 0, time.UTC), 2)
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		// insert book_bin
		var cover []byte
		cover, err = ioutil.ReadFile("../apple.jpg")
		if err != nil {
			return err
		}
		insert = DB.Insert(BOOK_BIN).
			Columns(BOOK_BIN_C_ID, BOOK_BIN_C_VERSION, BOOK_BIN_C_HARDCOVER).
			Values(1, 1, cover)
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		cover, err = ioutil.ReadFile("../cook-owl.png")
		if err != nil {
			return err
		}
		insert = DB.Insert(BOOK_BIN).
			Columns(BOOK_BIN_C_ID, BOOK_BIN_C_VERSION, BOOK_BIN_C_HARDCOVER).
			Values(2, 1, cover)
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		cover, err = ioutil.ReadFile("../scrapbook.png")
		if err != nil {
			return err
		}
		insert = DB.Insert(BOOK_BIN).
			Columns(BOOK_BIN_C_ID, BOOK_BIN_C_VERSION, BOOK_BIN_C_HARDCOVER).
			Values(3, 1, cover)
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
			Columns(AUTHOR_C_ID, AUTHOR_C_VERSION, AUTHOR_C_NAME, AUTHOR_C_SECRET).
			Values(1, 1, "John Doe", "@xpto")
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		insert.Values(2, 1, "Jane Doe", "947590245")
		_, err = insert.Execute()
		if err != nil {
			return err
		}

		insert.Values(3, 1, AUTHOR_UTF8_NAME, "#$%&!")
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

		// clear catalog
		if _, err = DB.Delete(CATALOG).Execute(); err != nil {
			return err
		}

		// insert publisher
		insert := DB.Insert(CATALOG).
			Columns(CATALOG_C_ID, CATALOG_C_VERSION, CATALOG_C_DOMAIN, CATALOG_C_CODE, CATALOG_C_VALUE)

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

func (tt Tester) RunSelectUTF8(TM ITransactionManager, t *testing.T) {
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
		t.Fatalf("Failed RunSelectUTF8: %s", err)
	} else if !ok || *publisher.Id != 2 || publisher.Version != 1 || *publisher.Name != PUBLISHER_UTF8_NAME {
		t.Fatalf("The record for publisher id 2, was not properly retrived. Retrived %s", publisher)
	}
}

func (tt Tester) RunRetrive(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// get the database context
	store := TM.Store()
	// the target entity
	var author Author
	ok, err := store.Retrive(&author, 3)
	if err != nil {
		t.Fatalf("Failed RunRetrive: %s", err)
	}
	if !ok || *author.Id != 3 || author.Version != 1 || *author.Name != AUTHOR_UTF8_NAME {
		t.Fatalf("Failed RunRetrive: The record for publisher id 3, was not properly retrived. Retrived %s", author.String())
	}
	if author.Secret != nil {
		t.Fatalf("Failed RunRetrive: Expected secret to be nil, found %s", *author.Secret)
	}
}

func (tt Tester) RunFindFirst(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// get the database context
	store := TM.Store()
	// the target entity
	var book Book
	ok, err := store.FindFirst(&book, Book{PublisherId: ext.Int64(1)})
	if err != nil {
		t.Fatalf("Failed RunFindFirst: %s", err)
	}
	if !ok {
		t.Fatalf("Failed RunFindFirst: Expected 1 books, got none.")
	}
}

func (tt Tester) RunFindAll(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// get the database context
	store := TM.Store()
	// the target entity
	var books []Book
	err := store.FindAll(&books, Book{PublisherId: ext.Int64(2)})
	if err != nil {
		t.Fatalf("Failed RunFindAll: %s", err)
	}
	if len(books) != 2 {
		t.Fatalf("Failed RunFindAll: Expected 2 books, got %s", len(books))
	}
}

func (tt Tester) RunOmitField(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// get the database context
	store := TM.Store()
	var author = Author{}
	ok, err := store.Retrive(&author, 1)
	if !ok || err != nil {
		t.Fatalf("Failed RunOmitField: Unable to Retrive - %s", err)
	}
	if author.Secret != nil {
		t.Fatal("Failed RunOmitField: Author.Secret was retrived")
	}
	name := "Paulo Quintans"
	author.Name = &name
	ok, err = store.Modify(&author)
	if !ok || err != nil {
		t.Fatalf("Failed RunOmitField: Failed Modify - %s", err)
	}

	// check the unchanged value of secret
	var auth Author
	ok, err = store.Query(AUTHOR).All().Where(AUTHOR_C_ID.Matches(1)).SelectTo(&auth)
	if !ok || err != nil {
		t.Fatalf("Failed RunOmitField: Unable to query - %s", err)
	}
	if *auth.Name != name || auth.Secret == nil {
		t.Fatalf("Failed RunOmitField: failed to modify using an omited field. Name: %v, Secret: %v, Author: %s", *auth.Name, auth.Secret, &auth)
	}
}

func (tt Tester) RunModifyField(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// get the database context
	store := TM.Store()
	var book Book
	store.Retrive(&book, 1)
	price := book.Price * 0.8
	book.SetPrice(price)
	ok, err := store.Modify(&book)
	if !ok || err != nil {
		t.Fatalf("Failed RunModifyField: Unable to query - %s", err)
	}
	store.Retrive(&book, 1)
	if book.Price != price {
		t.Fatalf("Failed RunModifyField: Expected price %v, got %v", price, book.Price)
	}
}

func (tt Tester) RunRemoveAll(TM ITransactionManager, t *testing.T) {
	ResetDB2(TM)

	// get the database context
	store := TM.Store()
	// the target entity
	affected, err := store.RemoveAll(Project{StatusCod: ext.String("DEV")})
	if err != nil {
		t.Fatalf("Failed RunRemoveAll: %s", err)
	}
	if affected != 2 {
		t.Fatalf("Failed RunRemoveAll: Expected 2 deleted Projects, got %s", affected)
	}
}

func (tt Tester) RunInsertReturningKey(TM ITransactionManager, t *testing.T) {
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

func (tt Tester) RunInsertStructReturningKey(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	var err error
	if err = TM.Transaction(func(store IDb) error {
		var pub Publisher
		pub.Name = ext.String("Untited Editors")
		key, err := store.Insert(PUBLISHER).Submit(&pub) // passing as a pointer
		if err != nil {
			return err
		}

		if key == 0 {
			t.Fatal("The Auto Insert Key for the ID column was not retrived")
		}

		if key != *pub.Id {
			t.Fatal("The Auto Insert Key for the ID field was not set")
		}

		var pubPtr = new(Publisher)
		pubPtr.Name = ext.String("Untited Editors")
		key, err = store.Insert(PUBLISHER).Submit(pubPtr)
		if err != nil {
			return err
		}

		if key == 0 {
			t.Fatal("The Auto Insert Key for the ID column was not retrived")
		}

		pub = Publisher{}
		pubPtr.Name = ext.String("Untited Editors")
		err = store.Create(&pub)
		if err != nil {
			return err
		}

		if pub.Id == nil || *pub.Id == 0 {
			t.Fatal("The Auto Insert Key for the ID column was not set")
		}

		if pub.Version == 0 {
			t.Fatal("Version column was not set")
		}

		return nil
	}); err != nil {
		t.Fatalf("Failed Struct Insert Return Key: %s", err)
	}
}

func (tt Tester) RunSimpleUpdate(TM ITransactionManager, t *testing.T) {
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

func (tt Tester) RunStructUpdate(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	var err error
	if err = TM.Transaction(func(store IDb) error {
		var publisher Publisher
		publisher.Name = ext.String("Untited Editors")
		publisher.Id = ext.Int64(1)
		publisher.Version = 1
		affectedRows, err := store.Update(PUBLISHER).Submit(&publisher) // passing as a pointer
		if err != nil {
			t.Fatalf("Failed RunStructUpdate: %s", err)
		}

		if affectedRows != 1 {
			t.Fatal("The record was not updated")
		}

		if publisher.Version != 2 {
			t.Fatalf("Expected Version = 2, got %v", publisher.Version)
		}

		publisher.Name = ext.String("Super Duper Test")
		ok, err := store.Modify(&publisher)
		if err != nil {
			t.Fatalf("Failed RunStructUpdate: %s", err)
		}

		if !ok {
			t.Fatal("The record was not Modifyied")
		}

		if publisher.Version != 3 {
			t.Fatalf("Expected Version = 3, got %v", publisher.Version)
		}

		return nil
	}); err != nil {
		t.Fatalf("Failed Update Test: %s", err)
	}
}

func (tt Tester) RunStructSaveAndRetrive(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	var err error
	if err = TM.Transaction(func(store IDb) error {
		var publisher Publisher
		// === save insert ===
		publisher.Name = ext.String("Super Duper Test")
		ok, err := store.Save(&publisher)
		if err != nil {
			t.Fatalf("Failed RunStructSaveAndRetrive: %s", err)
		}

		if !ok {
			t.Fatal("The record was not Saved")
		}

		if publisher.Version != 1 {
			t.Fatalf("Expected Version = 1, got %v", publisher.Version)
		}

		// === check insert ===
		var oldPub Publisher
		ok, err = store.Retrive(&oldPub, publisher.Id)
		if err != nil {
			t.Fatalf("Failed RunStructSaveAndRetrive: %s", err)
		}

		if !ok {
			t.Fatal("The record was not Saved")
		}

		if publisher.Version != oldPub.Version {
			t.Fatalf("Expected same Version = 1, got %v", oldPub.Version)
		}

		// === save update ===
		publisher.Name = ext.String("UPDDATE: Super Duper Test")
		ok, err = store.Save(&publisher)
		if err != nil {
			t.Fatalf("Failed RunStructSaveAndRetrive: %s", err)
		}

		if !ok {
			t.Fatal("The record was not Saved")
		}

		if publisher.Version != 2 {
			t.Fatalf("Expected Version = 2, got %v", publisher.Version)
		}

		// === check update ===
		oldPub = Publisher{}
		ok, err = store.Retrive(&oldPub, publisher.Id)
		if err != nil {
			t.Fatalf("Failed RunStructSaveAndRetrive: %s", err)
		}

		if !ok {
			t.Fatal("The record was not Saved")
		}

		if publisher.Version != oldPub.Version {
			t.Fatalf("Expected same Version, got %v", oldPub.Version)
		}

		// === check optimistic lock ===
		publisher.Version = 1 // invalid version
		ok, err = store.Save(&publisher)
		fail, _ := err.(*dbx.OptimisticLockFail)
		if fail == nil {
			t.Fatalf("Failed RunStructSaveAndRetrive: %s", err)
		}

		if ok {
			t.Fatal("The record was Saved, without an optimistic lock fail")
		}

		return nil
	}); err != nil {
		t.Fatalf("Failed Update Test: %s", err)
	}
}

func (tt Tester) RunUpdateSubquery(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	if err := TM.Transaction(func(store IDb) error {
		sub := store.Query(BOOK).Alias("b").
			Column(AsIs(nil)).
			Where(
				BOOK_C_PUBLISHER_ID.Matches(Col(BOOK_C_ID).For("a")),
				BOOK_C_PRICE.Greater(15),
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

func (tt Tester) RunSimpleDelete(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	if err := TM.Transaction(func(store IDb) error {
		// clears any relation with book id = 2
		store.Delete(AUTHOR_BOOK).Where(AUTHOR_BOOK_C_BOOK_ID.Matches(2)).Execute()
		store.Delete(BOOK_BIN).Where(BOOK_BIN_C_ID.Matches(2)).Execute()

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

func (tt Tester) RunStructDelete(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	if err := TM.Transaction(func(store IDb) error {
		// clears any relation with book id = 2
		store.Delete(AUTHOR_BOOK).Where(AUTHOR_BOOK_C_BOOK_ID.Matches(2)).Execute()
		store.Delete(BOOK_BIN).Where(BOOK_BIN_C_ID.Matches(2)).Execute()

		var book Book
		book.Id = ext.Int64(2)
		book.Version = 1
		affectedRows, err := store.Delete(BOOK).Submit(book)
		if err != nil {
			t.Fatalf("Failed RunStructDelete (id=2): %s", err)
		}
		if affectedRows != 1 {
			t.Fatal("The record was not deleted")
		}

		// short version
		// clears any relation with book id = 3
		store.Delete(AUTHOR_BOOK).Where(AUTHOR_BOOK_C_BOOK_ID.Matches(3)).Execute()
		store.Delete(BOOK_BIN).Where(BOOK_BIN_C_ID.Matches(3)).Execute()

		*book.Id = 3
		var ok bool
		ok, err = store.Remove(book)
		if err != nil {
			t.Fatalf("Failed RunStructDelete (id=3): %s", err)
		}
		if !ok {
			t.Fatal("The record was not deleted")
		}

		return nil
	}); err != nil {
		t.Fatalf("Failed ... Test: %s", err)
	}
}

func (tt Tester) RunSelectInto(TM ITransactionManager, t *testing.T) {
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

func (tt Tester) RunSelectTree(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	var publisher Publisher
	ok, err := store.Query(PUBLISHER).
		All().
		Outer(PUBLISHER_A_BOOKS).
		Fetch(). // add all columns off book in the query
		Where(PUBLISHER_C_ID.Matches(2)).
		SelectTree(&publisher)
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

func (tt Tester) RunSelectTreeTwoBranches(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	var book Book
	ok, err := store.Query(BOOK).
		All().
		Inner(BOOK_A_PUBLISHER).Fetch().
		Inner(BOOK_A_BOOK_BIN).Fetch().
		Where(BOOK_C_ID.Matches(1)).
		SelectTree(&book)

	if err != nil {
		t.Fatalf("%s", err)
	} else if !ok || book.Id == nil {
		t.Fatal("The record for publisher id 1, was not retrived")
	} else {
		// check list size of books
		if book.Publisher == nil {
			t.Fatalf("The publisher for book 1 was not retrived")
		}
		if book.BookBin == nil {
			t.Fatalf("The binary for book 1 was not retrived")
		}
		if len(book.BookBin.Hardcover) == 0 {
			t.Fatalf("The hardcover for book 1 was not retrived")
		}
	}
}

func (tt Tester) RunSelectFlatTree(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	var publisher Publisher
	ok, err := store.Query(PUBLISHER).
		All().
		Outer(PUBLISHER_A_BOOKS).
		Fetch(). // add all columns off book in the query
		Where(PUBLISHER_C_ID.Matches(2)).
		SelectFlatTree(&publisher)
	if err != nil {
		t.Fatalf("%s", err)
	} else if !ok || publisher.Id == nil {
		t.Fatal("The record for publisher id 2, was not retrived")
	} else {
		// check list size of books
		if len(publisher.Books) != 1 {
			t.Fatalf("The list of books for the publisher with id 2 was incorrectly retrived. Expected 1 got %v", len(publisher.Books))
		}
	}
}

func (tt Tester) RunListInto(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	books := make([]*Book, 0) // mandatory use pointers
	_, err := store.Query(BOOK).
		All().
		ListInto(func(book *Book) {
			books = append(books, book)
		})
	if err != nil {
		t.Fatalf("Failed List Test: %s", err)
	}

	if len(books) != 3 {
		t.Fatalf("Expected 3 returned books, but got %v", len(books))
	}

	for _, v := range books {
		if v.Id == nil {
			t.Fatalf("A book has invalid Id and therefore was not retrived")
		}
	}

	var bks []*Book
	err = store.Query(BOOK).
		All().
		List(&bks)
	if err != nil {
		t.Fatalf("Failed List Test: %s", err)
	}

	if len(bks) != 3 {
		t.Fatalf("Expected 3 returned books, but got %v", len(bks))
	}

	for _, v := range bks {
		if v.Id == nil {
			t.Fatalf("A book has invalid Id and therefore was not retrived")
		}
	}
}

func (tt Tester) RunListOf(TM ITransactionManager, t *testing.T) {
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

func (tt Tester) RunListFlatTree(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()

	var publishers []*Publisher
	err := store.Query(PUBLISHER).
		All().
		Outer(PUBLISHER_A_BOOKS).
		Fetch(). // add all columns off book in the query
		Where(PUBLISHER_C_ID.Matches(2)).
		ListFlatTree(&publishers)

	if err != nil {
		t.Fatalf("%s", err)
	}

	if len(publishers) != 2 {
		t.Fatalf("The record for publisher id 2, was not retrived. Expected collection size of 2, got %v", len(publishers))
	}

	for _, publisher := range publishers {
		// check list size of books
		if publisher.Id == nil {
			t.Fatalf("A book has invalid Id and therefore was not retrived")
		}
		if len(publisher.Books) != 1 {
			t.Fatalf("The list of books for the publisher with id 2 was incorrectly retrived. Expected 1 got %v", len(publisher.Books))
		}
	}

	publishers = make([]*Publisher, 0)
	err = store.Query(PUBLISHER).
		All().
		Outer(PUBLISHER_A_BOOKS).
		Fetch(). // add all columns off book in the query
		Where(PUBLISHER_C_ID.Matches(2)).
		ListFlatTree(func(publisher *Publisher) {
			publishers = append(publishers, publisher)
		})

	if err != nil {
		t.Fatalf("%s", err)
	}

	if len(publishers) != 2 {
		t.Fatalf("The record for publisher id 2, was not retrived. Expected collection size of 2, got %v", len(publishers))
	}

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

func (tt Tester) RunListTreeOf(TM ITransactionManager, t *testing.T) {
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

func (tt Tester) RunListForSlice(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	var names []string
	err := store.Query(PUBLISHER).
		Column(PUBLISHER_C_NAME).
		List(&names)
	if err != nil {
		t.Fatalf("Failed RunListSlice: %s", err)
	}

	if len(names) != 2 {
		t.Fatalf("Expected 2 Publisher names, but got %v", len(names))
	}

	for k, v := range names {
		logger.Debugf("names[%v] = %s", k, v)
		if v == "" {
			t.Fatalf("Expected a value for names[%v], but got an empty string", k)
		}
	}
}

func (tt Tester) RunListSimple(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	names := make([]string, 0)
	var name string
	err := store.Query(PUBLISHER).
		Column(PUBLISHER_C_NAME).
		ListSimple(func() {
			names = append(names, name)
		}, &name)
	if err != nil {
		t.Fatalf("Failed TestListSimple: %s", err)
	}

	if len(names) != 2 {
		t.Fatalf("Expected 2 Publisher names, but got %v", len(names))
	}
}

func (tt Tester) RunSearchedCase(TM ITransactionManager, t *testing.T) {
	// skip if it is Firebird
	if tt.DbName == Firebird {
		return
	}

	ResetDB(TM)

	var dtos []struct {
		Name           string
		Classification string
	}

	store := TM.Store()
	err := store.Query(BOOK).
		Column(BOOK_C_NAME).
		Column(
			If(BOOK_C_PRICE.Greater(20)).Then("expensive").
				If(BOOK_C_PRICE.Range(10, 20)).Then("normal").
				Else("cheap").
				End(),
		).As("Classification").
		List(&dtos)

	if err != nil {
		t.Fatalf("Failed RunSearchedCase: %s", err)
	}

	if len(dtos) != 3 {
		t.Fatalf("Expected 3 Books, but got %v", len(dtos))
	}

	for k, v := range dtos {
		logger.Debugf("dtos[%v] = %+v", k, v)
	}
}

func (tt Tester) RunSimpleCase(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	var sale float64
	store := TM.Store()
	_, err := store.Query(BOOK).
		Column(
			Sum(
				Case(BOOK_C_NAME).
					When("Scrapbook").Then(10).
					Else(AsIs(20)). // showing off AsIs(): value is written as is to the query
					End(),
			),
		).SelectInto(&sale)

	if err != nil {
		t.Fatalf("Failed RunSimpleCase: %s", err)
	}

	if sale != 50 {
		t.Fatalf("Expected sale of 50, but got %v", sale)
	}
}

func (tt Tester) RunColumnSubquery(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	subquery := store.Query(BOOK).Alias("b").
		Column(Sum(BOOK_C_PRICE)).
		Where(
			BOOK_C_PUBLISHER_ID.Matches(Col(PUBLISHER_C_ID).For("p")),
		)

	var dtos []*Dto
	err := store.Query(PUBLISHER).Alias("p").
		Column(PUBLISHER_C_NAME).
		Column(subquery).As("Value").
		List(&dtos)

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

func (tt Tester) RunWhereSubquery(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	subquery := store.Query(BOOK).
		Distinct().
		Column(BOOK_C_PUBLISHER_ID).
		Where(
			BOOK_C_PRICE.LesserOrMatch(10),
		)

	var dtos []*Dto
	err := store.Query(PUBLISHER).
		Column(PUBLISHER_C_NAME).
		Inner(PUBLISHER_A_BOOKS).
		Include(BOOK_C_NAME).As("OtherName").
		Include(BOOK_C_PRICE).As("Value").
		Join().
		Where(PUBLISHER_C_ID.In(subquery)).
		List(&dtos)

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

func (tt Tester) RunInnerOn(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// gets all publishers that had a book published before 2013
	store := TM.Store()
	var publishers = make([]*Publisher, 0)
	_, err := store.Query(PUBLISHER).
		All().
		Distinct().
		Inner(PUBLISHER_A_BOOKS).
		On(BOOK_C_PUBLISHED.Lesser(time.Date(2013, time.January, 1, 0, 0, 0, 0, time.UTC))).
		Join().
		ListInto(func(publisher *Publisher) {
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

func (tt Tester) RunInnerOn2(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	var publishers []*Publisher
	err := store.Query(PUBLISHER).
		All().
		Distinct().
		Inner(PUBLISHER_A_BOOKS).
		On(BOOK_C_PUBLISHED.Lesser(time.Date(2013, time.January, 1, 0, 0, 0, 0, time.UTC))).
		Inner(BOOK_A_AUTHORS).
		On(AUTHOR_C_NAME.Like("%Doe")).
		Join().
		List(&publishers)

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

func (tt Tester) RunOuterFetchOrder(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	logger.Debugf("Running RunOuterFetchOrder")

	store := TM.Store()
	result, err := store.Query(PUBLISHER).
		All().
		Order(PUBLISHER_C_ID).
		Outer(PUBLISHER_A_BOOKS).OrderBy(BOOK_C_ID).       // order a column belonging to BOOK
		Outer(BOOK_A_AUTHORS).OrderBy(AUTHOR_C_ID).Desc(). // order a column belonging to AUTHOR
		Fetch().                                           // this marks the end of the branch and that the results should populate a struct tree
		ListTreeOf((*Publisher)(nil))

	if err != nil {
		t.Fatalf("Failed RunOuterFetchOrder: %s", err)
	}

	publishers := result.AsSlice().([]*Publisher)

	if len(publishers) != 2 {
		t.Fatalf("Expected 2 Publishers, but got %v", len(publishers))
	}

	pub := publishers[0]
	if len(pub.Books) != 1 {
		t.Fatalf("Expected 1 Book for Publishers %s, but got %v", *pub.Name, len(pub.Books))
	}

	if *publishers[0].Id != 1 {
		t.Fatalf("The result order for publisher is not correct. Expected record with id 1 in the first position, but got %v", *publishers[0].Id)
	}

	book := pub.Books[0]
	if len(book.Authors) != 1 {
		t.Fatalf("Expected 1 Author for Book %s, but got %v", book.Name, len(book.Authors))
	}

	pub = publishers[1]
	if len(pub.Books) != 2 {
		t.Fatalf("Expected 2 Book for Publishers %s, but got %v", *pub.Name, len(pub.Books))
	}

	book = pub.Books[0]
	if len(book.Authors) != 2 {
		t.Fatalf("Expected 2 Author for Book %s, but got %v", book.Name, len(book.Authors))
	}

	book = pub.Books[1]
	if len(book.Authors) != 2 {
		t.Fatalf("Expected 2 Author for Book %s, but got %v", book.Name, len(book.Authors))
	}

	if *book.Authors[0].Id != 2 {
		t.Fatalf("The result order of authors is not correct. Expected record with id 2 in the first position, but got %v", *publishers[0].Id)
	}

	for k, v := range publishers {
		logger.Debugf("publishers[%v] = %s", k, v.String())
		if v.Id == nil {
			t.Fatal("Expected a valid Id, but got nil")
		}
	}
}

func (tt Tester) RunOuterFetchOrderAs(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	logger.Debugf("Running RunOuterFetchOrderAs")

	store := TM.Store()
	result, err := store.Query(PUBLISHER).
		All().
		Outer(PUBLISHER_A_BOOKS).As("book").
		Outer(BOOK_A_AUTHORS).As("auth").
		Fetch().
		Order(PUBLISHER_C_ID). // main table
		OrderAs(BOOK_C_ID.For("book")).
		OrderAs(AUTHOR_C_ID.For("auth")).Desc().
		ListTreeOf((*Publisher)(nil))

	if err != nil {
		t.Fatalf("Failed RunOuterFetchOrderAs: %s", err)
	}

	publishers := result.AsSlice().([]*Publisher)

	if len(publishers) != 2 {
		t.Fatalf("Expected 2 Publishers, but got %v", len(publishers))
	}

	pub := publishers[0]
	if len(pub.Books) != 1 {
		t.Fatalf("Expected 1 Book for Publishers %s, but got %v", *pub.Name, len(pub.Books))
	}

	if *publishers[0].Id != 1 {
		t.Fatalf("The result order for publisher is not correct. Expected record with id 1 in the first position, but got %v", *publishers[0].Id)
	}

	book := pub.Books[0]
	if len(book.Authors) != 1 {
		t.Fatalf("Expected 1 Author for Book %s, but got %v", book.Name, len(book.Authors))
	}

	pub = publishers[1]
	if len(pub.Books) != 2 {
		t.Fatalf("Expected 2 Book for Publishers %s, but got %v", *pub.Name, len(pub.Books))
	}

	book = pub.Books[0]
	if len(book.Authors) != 2 {
		t.Fatalf("Expected 2 Author for Book %s, but got %v", book.Name, len(book.Authors))
	}

	book = pub.Books[1]
	if len(book.Authors) != 2 {
		t.Fatalf("Expected 2 Author for Book %s, but got %v", book.Name, len(book.Authors))
	}

	if *book.Authors[0].Id != 2 {
		t.Fatalf("The result order of authors is not correct. Expected record with id 2 in the first position, but got %v", *publishers[0].Id)
	}

	for k, v := range publishers {
		logger.Debugf("publishers[%v] = %s", k, v.String())
		if v.Id == nil {
			t.Fatal("Expected a valid Id, but got nil")
		}
	}
}

func (tt Tester) RunGroupBy(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	var dtos []*Dto
	err := store.Query(PUBLISHER).
		Column(PUBLISHER_C_NAME).
		Outer(PUBLISHER_A_BOOKS).
		Include(Sum(BOOK_C_PRICE)).As("Value").
		Join().
		GroupByPos(1).
		List(&dtos)

	if err != nil {
		t.Fatalf("Failed RunGroupBy: %s", err)
	}

	if len(dtos) != 2 {
		t.Fatalf("Expected 2 Publisher names, but got %v", len(dtos))
	}

	for k, v := range dtos {
		logger.Debugf("dtos[%v] = %+v", k, *v)
	}
}

func (tt Tester) RunOrderBy(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	var publishers = make([]*Publisher, 0)
	_, err := store.Query(PUBLISHER).
		All().
		OrderBy(PUBLISHER_C_NAME).
		ListInto(func(publisher *Publisher) {
			publishers = append(publishers, publisher)
		})

	if err != nil {
		t.Fatalf("Failed RunOrderBy: %s", err)
	}

	if len(publishers) != 2 {
		t.Fatalf("Expected 2 Publisher names, but got %v", len(publishers))
	}

	if *publishers[0].Id != 2 {
		t.Fatalf("The result order is not correct. Expected record with id 2 in the first position, but got %v", *publishers[0].Id)
	}

	for k, v := range publishers {
		logger.Debugf("publishers[%v] = %s", k, v.String())
		if v.Id == nil {
			t.Fatal("Expected a valid Id, but got nil")
		}
	}
}

func (tt Tester) RunPagination(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	store := TM.Store()
	var publishers []*Publisher
	err := store.Query(PUBLISHER).
		All().
		Outer(PUBLISHER_A_BOOKS, BOOK_A_AUTHORS).
		Fetch().
		Order(PUBLISHER_C_NAME).
		Skip(2).  // skip the first 2 records
		Limit(3). // limit to 3 records
		ListFlatTree(&publishers)

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

func (tt Tester) RunAssociationDiscriminator(TM ITransactionManager, t *testing.T) {
	ResetDB2(TM)

	store := TM.Store()
	result, err := store.Query(PROJECT).
		All().
		Inner(PROJECT_A_EMPLOYEE).
		Fetch().
		Order(PROJECT_C_NAME).
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

func (tt Tester) RunAssociationDiscriminatorReverse(TM ITransactionManager, t *testing.T) {
	ResetDB2(TM)

	store := TM.Store()
	result, err := store.Query(EMPLOYEE).
		All().
		Inner(EMPLOYEE_A_PROJECT).
		Fetch().
		Order(EMPLOYEE_C_NAME).
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

func (tt Tester) RunTableDiscriminator(TM ITransactionManager, t *testing.T) {
	ResetDB3(TM)

	store := TM.Store()
	var statuses []*Status
	err := store.Query(STATUS).
		All().
		List(&statuses)

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
	status.Code = ext.String("X")
	status.Description = ext.String("Unknown")
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
	status.Code = ext.String("X")
	status.Description = ext.String("Unknown")
	tmp, err = store.Insert(STATUS).Submit(status)
	if err != nil {
		t.Fatalf("Failed Insert in TestTableDiscriminator: %s", err)
	}
	if tmp == 0 {
		t.Fatal("Expected Id different of 0")
	}

}

func (tt Tester) RunJoinTableDiscriminator(TM ITransactionManager, t *testing.T) {
	ResetDB3(TM)

	store := TM.Store()
	var result []*Project
	err := store.Query(PROJECT).
		All().
		Outer(PROJECT_A_STATUS).
		Fetch().
		ListFlatTree(&result)

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

func (tt Tester) RunCustomFunction(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// get the database context
	store := TM.Store()
	var books []*Book
	err := store.Query(BOOK).
		All().
		Where(
			SecondsDiff(
				time.Date(2013, time.July, 24, 0, 0, 0, 0, time.UTC),
				BOOK_C_PUBLISHED,
			).
				Greater(1000),
		).
		List(&books)

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

func (tt Tester) RunRawSQL1(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// get the database connection
	dba := dbx.NewSimpleDBA(TM.Store().GetConnection())
	var result []string

	_, err := dba.QueryInto(RAW_SQL,
		func(name string) { // we calso use pointers: func(name *string)
			result = append(result, name)
		}, "%book")

	if err != nil {
		t.Fatalf("Failed TestRawSQL1: %s", err)
	}

	for k, v := range result {
		logger.Debugf("books[%v] = %s", k, v)
		if v == "" {
			t.Fatal("Expected a valid Name, but got empty")
		}
	}
}

func (tt Tester) RunRawSQL2(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// get the database connection
	dba := dbx.NewSimpleDBA(TM.Store().GetConnection())
	var result []string

	err := dba.QueryClosure(RAW_SQL,
		func(rows *sql.Rows) error {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			result = append(result, name)
			return nil
		}, "%book")

	if err != nil {
		t.Fatalf("Failed TestRawSQL2: %s", err)
	}

	for k, v := range result {
		logger.Debugf("books[%v] = %s", k, v)
		if v == "" {
			t.Fatal("Expected a valid Name, but got empty")
		}
	}
}

func (tt Tester) RunHaving(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// get the database context
	store := TM.Store()
	sales := make([]*PublisherSales, 0)
	_, err := store.Query(PUBLISHER).
		Column(PUBLISHER_C_NAME).
		Outer(PUBLISHER_A_BOOKS).
		Include(Sum(BOOK_C_PRICE)).As("ThisYear").
		Join().
		GroupByPos(1).
		Having(Alias("ThisYear").Greater(30)).
		ListInto(func(sale *PublisherSales) {
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

func (tt Tester) RunUnion(TM ITransactionManager, t *testing.T) {
	ResetDB(TM)

	// get the database context
	store := TM.Store()
	sales := make([]*PublisherSales, 0)
	_, err := store.Query(PUBLISHER).
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
		ListInto(func(sale *PublisherSales) {
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

func (tt Tester) RunDriverConverter(TM ITransactionManager, t *testing.T) {
	db := TM.Store()
	// clear catalog
	if _, err := db.Delete(CATALOG).Execute(); err != nil {
		t.Fatal(err)
	}

	p := Palette{
		Code:  "GRAY",
		Value: &Color{102, 101, 100},
	}

	db.Insert(CATALOG).Submit(&p)
	actual := getColor(db, *p.Id)

	expected := "102|101|100"
	if actual != expected {
		t.Fatalf("expected %s, got %s", expected, actual)
	}

	p2 := Palette{}
	db.Query(CATALOG).
		Where(CATALOG_C_ID.Matches(p.Id)).
		SelectTo(&p2)

	if *p.Value != *p2.Value {
		t.Fatalf("Expected %+v, got %+v", p.Value, p2.Value)
	}
}

func getColor(db IDb, id int64) string {
	var raw string
	db.Query(CATALOG).Column(CATALOG_C_VALUE).Where(CATALOG_C_ID.Matches(id)).SelectInto(&raw)
	return raw
}
