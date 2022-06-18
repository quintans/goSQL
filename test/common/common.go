package common

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/jmoiron/sqlx"
	"github.com/quintans/faults"
	"github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/dbx"
	"github.com/quintans/toolkit/ext"
	"github.com/quintans/toolkit/log"
	"github.com/stretchr/testify/require"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/test")

// custom Db - for setting default parameters
func NewMyDb(connection dbx.IConnection, translator db.Translator, cache *sync.Map, lang string) *MyDb {
	baseDb := db.NewDb(connection, translator, cache)
	return &MyDb{baseDb, lang}
}

type MyDb struct {
	*db.Db
	Lang string
}

func (d *MyDb) Query(table *db.Table) *db.Query {
	query := d.Overrider.Query(table)
	query.SetParameter("lang", d.Lang)
	return query
}

const TOKEN_SECONDSDIFF = "SECONDSDIFF"

// SecondsDiff Token factory
// first parameter is greater than the second
func SecondsDiff(left, right interface{}) *db.Token {
	return db.NewToken(TOKEN_SECONDSDIFF, left, right)
}

func init() {
	log.Register("/", log.DEBUG, log.NewConsoleAppender(false), log.NewFileAppender("db_test.log", 0, true, true))
}

var RAW_SQL string

func InitDB(driverName, dataSourceName string, translator db.Translator, initSqlFile string) (db.ITransactionManager, *sql.DB, error) {
	translator.RegisterConverter("color", ColorConverter{})

	mydb, err := Connect(driverName, dataSourceName)
	if err != nil {
		return nil, nil, err
	}

	if err := CreateTables(mydb, initSqlFile); err != nil {
		return nil, nil, err
	}

	return db.NewTransactionManager(
		// database
		mydb,
		// databse context factory
		func(c dbx.IConnection, cache *sync.Map) db.IDb {
			return NewMyDb(c, translator, cache, "pt")
		},
		// statement cache
		1000,
	), mydb, nil
}

func Connect(driverName, dataSourceName string) (*sql.DB, error) {
	mydb, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	// wake up the database pool
	err = mydb.Ping()
	if err != nil {
		return nil, faults.Wrapf(err, "Unable to Ping DB")
	}
	return mydb, nil
}

func CreateTables(db *sql.DB, initSqlFile string) error {
	logger.Infof("******* Creating tables *******\n")

	sql, err := ioutil.ReadFile(initSqlFile)
	if err != nil {
		return faults.Errorf("Unable to read file %s: %w", initSqlFile, err)
	}

	stmts := strings.Split(string(sql), ";\n")

	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		logger.Debug(stmt)
		if stmt != "" {
			_, err := db.Exec(stmt)
			if err != nil {
				return faults.Errorf("sql: %s: %w", stmt, err)
			}
		}
	}
	return nil
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
	Tm     db.ITransactionManager
}

func (tt Tester) RunAll(t *testing.T) {
	t.Run("RunRetrieveOther", tt.RunRetrieveOther)
	t.Run("RunEmbedded", tt.RunEmbedded)
	t.Run("RunEmbeddedPtr", tt.RunEmbeddedPtr)
	t.Run("RunConverter", tt.RunConverter)
	t.Run("RunQueryIntoUnexportedFields", tt.RunQueryIntoUnexportedFields)
	t.Run("RunSelectUTF8", tt.RunSelectUTF8)
	t.Run("RunRetrieve", tt.RunRetrieve)
	t.Run("RunFindFirst", tt.RunFindFirst)
	t.Run("RunFindAll", tt.RunFindAll)
	t.Run("RunOmitField", tt.RunOmitField)
	t.Run("RunModifyField", tt.RunModifyField)
	t.Run("RunRemoveAll", tt.RunRemoveAll)
	t.Run("RunInsertReturningKey", tt.RunInsertReturningKey)
	t.Run("RunInsertStructReturningKey", tt.RunInsertStructReturningKey)
	t.Run("RunSimpleUpdate", tt.RunSimpleUpdate)
	t.Run("RunStructUpdate", tt.RunStructUpdate)
	t.Run("RunStructSaveAndRetrieve", tt.RunStructSaveAndRetrieve)
	t.Run("RunUpdateSubquery", tt.RunUpdateSubquery)
	t.Run("RunSimpleDelete", tt.RunSimpleDelete)
	t.Run("RunStructDelete", tt.RunStructDelete)
	t.Run("RunSelectInto", tt.RunSelectInto)
	t.Run("RunSelectTree", tt.RunSelectTree)
	t.Run("RunSelectTreeTwoBranches", tt.RunSelectTreeTwoBranches)
	t.Run("RunSelectFlatTree", tt.RunSelectFlatTree)
	t.Run("RunListInto", tt.RunListInto)
	t.Run("RunListOf", tt.RunListOf)
	t.Run("RunList", tt.RunList) // <===
	t.Run("RunListFlatTree", tt.RunListFlatTree)
	t.Run("RunListTreeOf", tt.RunListTreeOf)
	t.Run("RunListForSlice", tt.RunListForSlice)
	t.Run("RunListSimple", tt.RunListSimple)
	t.Run("RunSimpleCase", tt.RunSimpleCase)
	t.Run("RunSearchedCase", tt.RunSearchedCase)
	t.Run("RunColumnSubquery", tt.RunColumnSubquery)
	t.Run("RunWhereSubquery", tt.RunWhereSubquery)
	t.Run("RunInnerOn", tt.RunInnerOn)
	t.Run("RunInnerOn2", tt.RunInnerOn2)
	t.Run("RunOuterFetchOrder", tt.RunOuterFetchOrder)
	t.Run("RunOuterFetchOrderAs", tt.RunOuterFetchOrderAs)
	t.Run("RunGroupBy", tt.RunGroupBy)
	t.Run("RunOrderBy", tt.RunOrderBy)
	t.Run("RunPagination", tt.RunPagination)
	t.Run("RunAssociationDiscriminator", tt.RunAssociationDiscriminator)
	t.Run("RunAssociationDiscriminatorReverse", tt.RunAssociationDiscriminatorReverse)
	t.Run("RunTableDiscriminator", tt.RunTableDiscriminator)
	t.Run("RunJoinTableDiscriminator", tt.RunJoinTableDiscriminator)
	t.Run("RunCustomFunction", tt.RunCustomFunction)
	t.Run("RunRawSQL1", tt.RunRawSQL1)
	t.Run("RunRawSQL2", tt.RunRawSQL2)
	t.Run("RunHaving", tt.RunHaving)
	t.Run("RunUnion", tt.RunUnion)
}

func ResetDB(TM db.ITransactionManager) {
	if err := TM.Transaction(func(DB db.IDb) error {
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

		insert.Values(3, 1, "Scrapbook", 6.5, time.Date(2012, time.April, 0o1, 0, 0, 0, 0, time.UTC), 2)
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
		fmt.Printf("%+v\n", err)
		panic(err)
	}
}

func ResetDB2(TM db.ITransactionManager) {
	if err := TM.Transaction(func(DB db.IDb) error {
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
			Columns(EMPLOYEE_C_ID, EMPLOYEE_C_VERSION, EMPLOYEE_C_FIRST_NAME).
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
		fmt.Printf("%+v\n", err)
		panic(err)
	}
}

func ResetDB3(TM db.ITransactionManager) {
	if err := TM.Transaction(func(DB db.IDb) error {
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
		fmt.Printf("%+v\n", err)
		panic(err)
	}
}

func (tt Tester) RunBench(driver string, dns string, table string, b *testing.B) {
	ResetDB(tt.Tm)

	const maxRows = 1000
	type Employee struct {
		Id        *int
		FirstName *string
		LastName  *string
	}

	err := tt.Tm.Transaction(func(DB db.IDb) error {
		for i := 1; i <= maxRows; i++ {
			err := DB.Create(&Employee{
				Id:        ext.Int(i),
				FirstName: ext.String("Paulo"),
				LastName:  ext.String("Pereira"),
			})

			require.NoError(b, err)
		}
		return nil
	})

	require.NoError(b, err)

	store := tt.Tm.Store()
	for n := 10; n <= maxRows; n *= 10 {
		q := fmt.Sprintf("SELECT * FROM EMPLOYEE ORDER BY id ASC LIMIT %d", n)

		b.Run(fmt.Sprintf("sqlx_%d", n), func(b *testing.B) {
			b.StopTimer()
			db, err := sqlx.Connect(driver, dns)
			require.NoError(b, err)
			db = db.Unsafe()
			defer db.Close()

			for i := 0; i < b.N; i++ {
				var emps []*Employee
				b.StartTimer()
				err := db.SelectContext(context.Background(), &emps, q)
				b.StopTimer()
				require.NoError(b, err)
				require.Len(b, emps, n)
			}
		})

		b.Run(fmt.Sprintf("gosql_%d", n), func(b *testing.B) {
			b.StopTimer()

			for i := 0; i < b.N; i++ {
				var emps []*Employee
				b.StartTimer()
				err := store.Query(EMPLOYEE).
					Column(EMPLOYEE_C_FIRST_NAME, EMPLOYEE_C_LAST_NAME).
					OrderBy(EMPLOYEE_C_ID).Asc().
					Limit(int64(n)).
					List(&emps)
				b.StopTimer()
				require.NoError(b, err)
				require.Len(b, emps, n)
			}
		})

		b.Run(fmt.Sprintf("gorm_%d", n), func(b *testing.B) {
			b.StopTimer()
			db, err := gorm.Open(driver, dns)
			db = db.Table(table).Order("ID ASC").Limit(n)
			db.SingularTable(true)
			require.NoError(b, err)
			defer db.Close()

			for i := 0; i < b.N; i++ {
				var emps []*Employee
				b.StartTimer()
				err := db.Find(&emps).Error
				require.NoError(b, err)
				b.StopTimer()
				require.Len(b, emps, n)
			}
		})
	}
}

func (tt Tester) RunSelectUTF8(t *testing.T) {
	ResetDB(tt.Tm)

	// get the database context
	store := tt.Tm.Store()
	// the target entity
	publisher := Publisher{}

	ok, err := store.Query(PUBLISHER).
		All().
		Where(PUBLISHER_C_ID.Matches(2)).
		SelectTo(&publisher)

	if err != nil {
		t.Fatalf("Failed RunSelectUTF8: %s", err)
	} else if !ok || *publisher.Id != 2 || publisher.Version != 1 || *publisher.Name != PUBLISHER_UTF8_NAME {
		t.Fatalf("The record for publisher id 2, was not properly retrieved. Retrieved %v", publisher)
	}
}

func (tt Tester) RunRetrieve(t *testing.T) {
	ResetDB(tt.Tm)

	// get the database context
	store := tt.Tm.Store()
	// the target entity
	var author Author
	ok, err := store.Retrieve(&author, 3)
	if err != nil {
		t.Fatalf("Failed RunRetrieve: %s", err)
	}
	if !ok || *author.Id != 3 || author.Version != 1 || *author.Name != AUTHOR_UTF8_NAME {
		t.Fatalf("Failed RunRetrieve: The record for publisher id 3, was not properly retrieved. Retrieved %s", author.String())
	}
	if author.Secret != nil {
		t.Fatalf("Failed RunRetrieve: Expected secret to be nil, found %s", *author.Secret)
	}
}

type NotAuthor struct {
	EntityBase
	Name *string
}

func (t *NotAuthor) TableName() string {
	return AUTHOR.Alias
}

func (tt Tester) RunRetrieveOther(t *testing.T) {
	ResetDB(tt.Tm)

	// get the database context
	store := tt.Tm.Store()
	// the target entity

	author := NotAuthor{}

	ok, err := store.Retrieve(&author, 3)
	if err != nil {
		t.Fatalf("Failed RunRetrieveOther: %s", err)
	}
	if !ok || *author.Id != 3 || author.Version != 1 || *author.Name != AUTHOR_UTF8_NAME {
		t.Fatalf("Failed RunRetrieveOther: The record for publisher id 3, was not properly retrieved. Retrieved %+v", author)
	}
}

func (tt Tester) RunQueryIntoUnexportedFields(t *testing.T) {
	ResetDB(tt.Tm)

	// get the database context
	store := tt.Tm.Store()
	// the target entity
	author := struct {
		EntityBase
		name *string
	}{}
	ok, err := store.Query(AUTHOR).
		All().
		Where(AUTHOR_C_ID.Matches(3)).
		SelectTo(&author)
	if err != nil {
		t.Fatalf("Failed RunRetrieveIntoUnexportedFields: %s", err)
	}
	if !ok || *author.Id != 3 || author.Version != 1 || *author.name != AUTHOR_UTF8_NAME {
		t.Fatalf("Failed RunRetrieveIntoUnexportedFields: The record for publisher id 3, was not properly retrieved. Retrieved %+v", author)
	}
}

func (tt Tester) RunFindFirst(t *testing.T) {
	ResetDB(tt.Tm)

	// get the database context
	store := tt.Tm.Store()
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

func (tt Tester) RunFindAll(t *testing.T) {
	ResetDB(tt.Tm)

	// get the database context
	store := tt.Tm.Store()
	// the target entity
	var books []Book
	err := store.FindAll(&books, Book{PublisherId: ext.Int64(2)})
	if err != nil {
		t.Fatalf("Failed RunFindAll: %s", err)
	}
	if len(books) != 2 {
		t.Fatalf("Failed RunFindAll: Expected 2 books, got %v", len(books))
	}
}

func (tt Tester) RunOmitField(t *testing.T) {
	ResetDB(tt.Tm)

	// get the database context
	store := tt.Tm.Store()
	author := Author{}
	ok, err := store.Retrieve(&author, 1)
	if !ok || err != nil {
		t.Fatalf("Failed RunOmitField: Unable to Retrieve - %s", err)
	}
	if author.Secret != nil {
		t.Fatal("Failed RunOmitField: Author.Secret was retrieved")
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

func (tt Tester) RunModifyField(t *testing.T) {
	ResetDB(tt.Tm)

	// get the database context
	store := tt.Tm.Store()
	var book Book
	store.Retrieve(&book, 1)
	price := book.Price * 0.8
	book.SetPrice(price)
	ok, err := store.Modify(&book)
	if !ok || err != nil {
		t.Fatalf("Failed RunModifyField: Unable to query - %s", err)
	}
	store.Retrieve(&book, 1)
	if book.Price != price {
		t.Fatalf("Failed RunModifyField: Expected price %v, got %v", price, book.Price)
	}
}

func (tt Tester) RunRemoveAll(t *testing.T) {
	ResetDB2(tt.Tm)

	// get the database context
	store := tt.Tm.Store()
	// the target entity
	affected, err := store.RemoveAll(Project{StatusCod: ext.String("DEV")})
	if err != nil {
		t.Fatalf("Failed RunRemoveAll: %s", err)
	}
	if affected != 2 {
		t.Fatalf("Failed RunRemoveAll: Expected 2 deleted Projects, got %v", affected)
	}
}

func (tt Tester) RunInsertReturningKey(t *testing.T) {
	ResetDB(tt.Tm)

	var err error
	if err = tt.Tm.Transaction(func(store db.IDb) error {
		key, err := store.Insert(PUBLISHER).
			Columns(PUBLISHER_C_ID, PUBLISHER_C_VERSION, PUBLISHER_C_NAME).
			Values(nil, 1, "New Editions").
			Execute()
		if err != nil {
			return err
		}

		if key == 0 {
			t.Fatal("The Auto Insert Key for a null ID column was not retrieved")
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
			t.Fatal("The Auto Insert Key for a absent ID column was not retrieved")
		}

		logger.Debugf("The Auto Insert Key for a absent ID column was %v", key)

		return nil
	}); err != nil {
		t.Fatalf("Failed Insert Returning Key: %s", err)
	}
}

func (tt Tester) RunInsertStructReturningKey(t *testing.T) {
	ResetDB(tt.Tm)

	var err error
	if err = tt.Tm.Transaction(func(store db.IDb) error {
		var pub Publisher
		pub.Name = ext.String("Untited Editors")
		key, err := store.Insert(PUBLISHER).Submit(&pub) // passing as a pointer
		if err != nil {
			return err
		}

		if key == 0 {
			t.Fatal("The Auto Insert Key for the ID column was not retrieved")
		}

		if key != *pub.Id {
			t.Fatal("The Auto Insert Key for the ID field was not set")
		}

		pubPtr := new(Publisher)
		pubPtr.Name = ext.String("Untited Editors")
		key, err = store.Insert(PUBLISHER).Submit(pubPtr)
		if err != nil {
			return err
		}

		if key == 0 {
			t.Fatal("The Auto Insert Key for the ID column was not retrieved")
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

func (tt Tester) RunSimpleUpdate(t *testing.T) {
	ResetDB(tt.Tm)

	var err error
	if err = tt.Tm.Transaction(func(store db.IDb) error {
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

func (tt Tester) RunStructUpdate(t *testing.T) {
	ResetDB(tt.Tm)

	var err error
	if err = tt.Tm.Transaction(func(store db.IDb) error {
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

func (tt Tester) RunStructSaveAndRetrieve(t *testing.T) {
	ResetDB(tt.Tm)

	var err error
	if err = tt.Tm.Transaction(func(store db.IDb) error {
		var publisher Publisher
		// === save insert ===
		publisher.Name = ext.String("Super Duper Test")
		ok, err := store.Save(&publisher)
		if err != nil {
			t.Fatalf("Failed RunStructSaveAndRetrieve: %s", err)
		}

		if !ok {
			t.Fatal("The record was not Saved")
		}

		if publisher.Version != 1 {
			t.Fatalf("Expected Version = 1, got %v", publisher.Version)
		}

		// === check insert ===
		var oldPub Publisher
		ok, err = store.Retrieve(&oldPub, publisher.Id)
		if err != nil {
			t.Fatalf("Failed RunStructSaveAndRetrieve: %s", err)
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
			t.Fatalf("Failed RunStructSaveAndRetrieve: %s", err)
		}

		if !ok {
			t.Fatal("The record was not Saved")
		}

		if publisher.Version != 2 {
			t.Fatalf("Expected Version = 2, got %v", publisher.Version)
		}

		// === check update ===
		oldPub = Publisher{}
		ok, err = store.Retrieve(&oldPub, publisher.Id)
		if err != nil {
			t.Fatalf("Failed RunStructSaveAndRetrieve: %s", err)
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
			t.Fatalf("Failed RunStructSaveAndRetrieve: %s", err)
		}

		if ok {
			t.Fatal("The record was Saved, without an optimistic lock fail")
		}

		return nil
	}); err != nil {
		t.Fatalf("Failed Update Test: %s", err)
	}
}

func (tt Tester) RunUpdateSubquery(t *testing.T) {
	ResetDB(tt.Tm)

	if err := tt.Tm.Transaction(func(store db.IDb) error {
		sub := store.Query(BOOK).Alias("b").
			Column(db.AsIs(nil)).
			Where(
				BOOK_C_PUBLISHER_ID.Matches(db.Col(BOOK_C_ID).For("a")),
				BOOK_C_PRICE.Greater(15),
			)

		affectedRows, err := store.Update(PUBLISHER).Alias("a").
			Set(PUBLISHER_C_NAME, db.Upper(PUBLISHER_C_NAME)).
			Where(db.Exists(sub)).
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

func (tt Tester) RunSimpleDelete(t *testing.T) {
	ResetDB(tt.Tm)

	if err := tt.Tm.Transaction(func(store db.IDb) error {
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

func (tt Tester) RunStructDelete(t *testing.T) {
	ResetDB(tt.Tm)

	if err := tt.Tm.Transaction(func(store db.IDb) error {
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

func (tt Tester) RunSelectInto(t *testing.T) {
	ResetDB(tt.Tm)

	store := tt.Tm.Store()
	var name string
	ok, err := store.Query(PUBLISHER).
		Column(PUBLISHER_C_NAME).
		Where(PUBLISHER_C_ID.Matches(2)).
		SelectInto(&name)
	if err != nil {
		t.Fatalf("%s", err)
	} else if !ok || name != PUBLISHER_UTF8_NAME {
		t.Fatalf("Failed SelectInto. The name for publisher id 2, was not properly retrieved. Retrieved %s", name)
	}
}

func (tt Tester) RunSelectTree(t *testing.T) {
	ResetDB(tt.Tm)

	store := tt.Tm.Store()
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
		t.Fatal("The record for publisher id 2, was not retrieved")
	} else {
		// check list size of books
		if len(publisher.Books) != 2 {
			t.Fatalf("The list of books for the publisher with id 2 was incorrectly retrieved. Expected 2 got %v", len(publisher.Books))
		}
	}
}

func (tt Tester) RunSelectTreeTwoBranches(t *testing.T) {
	ResetDB(tt.Tm)

	store := tt.Tm.Store()
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
		t.Fatal("The record for publisher id 1, was not retrieved")
	} else {
		// check list size of books
		if book.Publisher == nil {
			t.Fatalf("The publisher for book 1 was not retrieved")
		}
		if book.BookBin == nil {
			t.Fatalf("The binary for book 1 was not retrieved")
		}
		if len(book.BookBin.Hardcover) == 0 {
			t.Fatalf("The hardcover for book 1 was not retrieved")
		}
	}
}

func (tt Tester) RunSelectFlatTree(t *testing.T) {
	ResetDB(tt.Tm)

	store := tt.Tm.Store()
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
		t.Fatal("The record for publisher id 2, was not retrieved")
	} else {
		// check list size of books
		if len(publisher.Books) != 1 {
			t.Fatalf("The list of books for the publisher with id 2 was incorrectly retrieved. Expected 1 got %v", len(publisher.Books))
		}
	}
}

func (tt Tester) RunListInto(t *testing.T) {
	ResetDB(tt.Tm)

	store := tt.Tm.Store()
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
			t.Fatalf("A book has invalid Id and therefore was not retrieved")
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
			t.Fatalf("A book has invalid Id and therefore was not retrieved")
		}
	}
}

func (tt Tester) RunListOf(t *testing.T) {
	ResetDB(tt.Tm)

	store := tt.Tm.Store()
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
				t.Fatalf("A book has invalid Id and therefore was not retrieved")
			}
		}
	}
}

func (tt Tester) RunList(t *testing.T) {
	ResetDB(tt.Tm)

	store := tt.Tm.Store()
	var books []*Book
	err := store.Query(BOOK).
		Column(BOOK_C_NAME, BOOK_C_PRICE).
		OrderBy(BOOK_C_ID).Asc().
		List(&books)
	require.NoError(t, err)
	require.Len(t, books, 3)
}

func (tt Tester) RunListFlatTree(t *testing.T) {
	ResetDB(tt.Tm)

	store := tt.Tm.Store()

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
		t.Fatalf("The record for publisher id 2, was not retrieved. Expected collection size of 2, got %v", len(publishers))
	}

	for _, publisher := range publishers {
		// check list size of books
		if publisher.Id == nil {
			t.Fatalf("A book has invalid Id and therefore was not retrieved")
		}
		if len(publisher.Books) != 1 {
			t.Fatalf("The list of books for the publisher with id 2 was incorrectly retrieved. Expected 1 got %v", len(publisher.Books))
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
		t.Fatalf("The record for publisher id 2, was not retrieved. Expected collection size of 2, got %v", len(publishers))
	}

	for _, publisher := range publishers {
		// check list size of books
		if publisher.Id == nil {
			t.Fatalf("A book has invalid Id and therefore was not retrieved")
		}
		if len(publisher.Books) != 1 {
			t.Fatalf("The list of books for the publisher with id 2 was incorrectly retrieved. Expected 1 got %v", len(publisher.Books))
		}
	}
}

func (tt Tester) RunListTreeOf(t *testing.T) {
	ResetDB(tt.Tm)

	store := tt.Tm.Store()
	publishers, err := store.Query(PUBLISHER).
		All().
		Outer(PUBLISHER_A_BOOKS).
		Fetch(). // add all columns off book in the query
		Where(PUBLISHER_C_ID.Matches(2)).
		ListTreeOf((*Publisher)(nil))
	if err != nil {
		t.Fatalf("%s", err)
	} else if publishers.Size() != 1 {
		t.Fatalf("The record for publisher id 2, was not retrieved. Expected collection size of 1, got %v", publishers.Size())
	} else {
		for e := publishers.Enumerator(); e.HasNext(); {
			publisher := e.Next().(*Publisher)
			// check list size of books
			if publisher.Id == nil {
				t.Fatalf("A book has invalid Id and therefore was not retrieved")
			}
			if len(publisher.Books) != 2 {
				t.Fatalf("The list of books for the publisher with id 2 was incorrectly retrieved. Expected 2 got %v", len(publisher.Books))
			}
		}
	}
}

func (tt Tester) RunListForSlice(t *testing.T) {
	ResetDB(tt.Tm)

	store := tt.Tm.Store()
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

func (tt Tester) RunListSimple(t *testing.T) {
	ResetDB(tt.Tm)

	store := tt.Tm.Store()
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

func (tt Tester) RunSearchedCase(t *testing.T) {
	// skip if it is Firebird
	if tt.DbName == Firebird {
		return
	}

	ResetDB(tt.Tm)

	var dtos []struct {
		Name           string
		Classification string
	}

	store := tt.Tm.Store()
	err := store.Query(BOOK).
		Column(BOOK_C_NAME).
		Column(
			db.If(BOOK_C_PRICE.Greater(20)).Then("expensive").
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

func (tt Tester) RunSimpleCase(t *testing.T) {
	ResetDB(tt.Tm)

	var sale float64
	store := tt.Tm.Store()
	_, err := store.Query(BOOK).
		Column(
			db.Sum(
				db.Case(BOOK_C_NAME).
					When("Scrapbook").Then(10).
					Else(db.AsIs(20)). // showing off AsIs(): value is written as is to the query
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

func (tt Tester) RunColumnSubquery(t *testing.T) {
	ResetDB(tt.Tm)

	store := tt.Tm.Store()
	subquery := store.Query(BOOK).Alias("b").
		Column(db.Sum(BOOK_C_PRICE)).
		Where(
			BOOK_C_PUBLISHER_ID.Matches(db.Col(PUBLISHER_C_ID).For("p")),
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

func (tt Tester) RunWhereSubquery(t *testing.T) {
	ResetDB(tt.Tm)

	store := tt.Tm.Store()
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

func (tt Tester) RunInnerOn(t *testing.T) {
	ResetDB(tt.Tm)

	// gets all publishers that had a book published before 2013
	store := tt.Tm.Store()
	publishers := make([]*Publisher, 0)
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

func (tt Tester) RunInnerOn2(t *testing.T) {
	ResetDB(tt.Tm)

	store := tt.Tm.Store()
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

func (tt Tester) RunOuterFetchOrder(t *testing.T) {
	ResetDB(tt.Tm)

	logger.Debugf("Running RunOuterFetchOrder")

	store := tt.Tm.Store()
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

func (tt Tester) RunOuterFetchOrderAs(t *testing.T) {
	ResetDB(tt.Tm)

	logger.Debugf("Running RunOuterFetchOrderAs")

	store := tt.Tm.Store()
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

func (tt Tester) RunGroupBy(t *testing.T) {
	ResetDB(tt.Tm)

	store := tt.Tm.Store()
	var dtos []*Dto
	err := store.Query(PUBLISHER).
		Column(PUBLISHER_C_NAME).
		Outer(PUBLISHER_A_BOOKS).
		Include(db.Sum(BOOK_C_PRICE)).As("Value").
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

func (tt Tester) RunOrderBy(t *testing.T) {
	ResetDB(tt.Tm)

	store := tt.Tm.Store()
	publishers := []*Publisher{}
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

func (tt Tester) RunPagination(t *testing.T) {
	ResetDB(tt.Tm)

	store := tt.Tm.Store()
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

func (tt Tester) RunAssociationDiscriminator(t *testing.T) {
	ResetDB2(tt.Tm)

	store := tt.Tm.Store()
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
			t.Fatalf("Expected Employee for project %v but got <nil>", v.Name)
		}
	}

	for k, v := range projects {
		logger.Debugf("Projects[%v] = %s", k, v.String())
		if v.Id == nil {
			t.Fatal("Expected a valid Id, but got nil")
		}
	}
}

func (tt Tester) RunAssociationDiscriminatorReverse(t *testing.T) {
	ResetDB2(tt.Tm)

	store := tt.Tm.Store()
	result, err := store.Query(EMPLOYEE).
		All().
		Inner(EMPLOYEE_A_PROJECT).
		Fetch().
		Order(EMPLOYEE_C_FIRST_NAME).
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
				t.Fatalf("Expected Project for emplyoee '%v' but got <nil>", *v.FirstName)
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

func (tt Tester) RunTableDiscriminator(t *testing.T) {
	ResetDB3(tt.Tm)

	store := tt.Tm.Store()
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
		logger.Debugf("Statuses[%v] = %s", k, v.String())
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

func (tt Tester) RunJoinTableDiscriminator(t *testing.T) {
	ResetDB3(tt.Tm)

	store := tt.Tm.Store()
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

func (tt Tester) RunCustomFunction(t *testing.T) {
	ResetDB(tt.Tm)

	// get the database context
	store := tt.Tm.Store()
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

func (tt Tester) RunRawSQL1(t *testing.T) {
	ResetDB(tt.Tm)

	// get the database connection
	dba := dbx.NewSimpleDBA(tt.Tm.Store().GetConnection())
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

func (tt Tester) RunRawSQL2(t *testing.T) {
	ResetDB(tt.Tm)

	// get the database connection
	dba := dbx.NewSimpleDBA(tt.Tm.Store().GetConnection())
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

func (tt Tester) RunHaving(t *testing.T) {
	ResetDB(tt.Tm)

	// get the database context
	store := tt.Tm.Store()
	sales := make([]*PublisherSales, 0)
	_, err := store.Query(PUBLISHER).
		Column(PUBLISHER_C_NAME).
		Outer(PUBLISHER_A_BOOKS).
		Include(db.Sum(BOOK_C_PRICE)).As("ThisYear").
		Join().
		GroupByPos(1).
		Having(db.Alias("ThisYear").Greater(30)).
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

func (tt Tester) RunUnion(t *testing.T) {
	ResetDB(tt.Tm)

	// get the database context
	store := tt.Tm.Store()
	sales := make([]*PublisherSales, 0)
	_, err := store.Query(PUBLISHER).
		Column(PUBLISHER_C_ID).
		Column(PUBLISHER_C_NAME).
		Outer(PUBLISHER_A_BOOKS).
		Include(db.Sum(db.Coalesce(BOOK_C_PRICE, 0))).As("ThisYear").
		On(
			db.Range(
				BOOK_C_PUBLISHED,
				time.Date(2013, time.January, 0o1, 0, 0, 0, 0, time.UTC),
				time.Date(2013, time.December, 31, 23, 59, 59, 1e9-1, time.UTC),
			),
		).
		Join().
		Column(db.AsIs(0)).As("PreviousYear").
		GroupByPos(1, 2).
		UnionAll(
			store.Query(PUBLISHER).Alias("u").
				Column(PUBLISHER_C_ID).
				Column(PUBLISHER_C_NAME).
				Outer(PUBLISHER_A_BOOKS).
				Column(db.AsIs(0)).As("ThisYear").
				Include(db.Sum(db.Coalesce(BOOK_C_PRICE, 0))).As("PreviousYear").
				On(
					db.Range(
						BOOK_C_PUBLISHED,
						time.Date(2012, time.January, 0o1, 0, 0, 0, 0, time.UTC),
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

func (tt Tester) RunConverter(t *testing.T) {
	db := tt.Tm.Store()
	// clear catalog
	if _, err := db.Delete(CATALOG).Execute(); err != nil {
		t.Fatalf("%+v", err)
	}

	p := Palette{
		Code:  "GRAY",
		Value: &Color{102, 101, 100},
	}

	if _, err := db.Insert(CATALOG).Submit(&p); err != nil {
		t.Fatalf("%+v", err)
	}
	actual := getColor(db, *p.Id)

	expected := "102|101|100"
	if actual != expected {
		t.Fatalf("expected %s, got %s", expected, actual)
	}

	p2 := Palette{}
	_, err := db.Query(CATALOG).
		Where(CATALOG_C_ID.Matches(p.Id)).
		SelectTo(&p2)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	if p2.Value == nil || *p.Value != *p2.Value {
		t.Fatalf("Expected %+v, got %+v", p.Value, p2.Value)
	}
}

func getColor(db db.IDb, id int64) string {
	var raw string
	db.Query(CATALOG).Column(CATALOG_C_VALUE).Where(CATALOG_C_ID.Matches(id)).SelectInto(&raw)
	return raw
}

func (tt Tester) RunEmbedded(t *testing.T) {
	db := tt.Tm.Store()
	// clear catalog
	if _, err := db.Delete(EMPLOYEE).Execute(); err != nil {
		t.Fatalf("%+v", err)
	}

	expectedFirst := "Paulo"
	expectedLast := "Pereira"
	p := Supervisor{
		FullName: FullNameVO{
			firstName: expectedFirst,
			lastName:  expectedLast,
		},
	}

	if _, err := db.Insert(EMPLOYEE).Submit(&p); err != nil {
		t.Fatalf("%+v", err)
	}
	actualFirst, actualLast := getNames(db, *p.Id)

	if actualFirst != expectedFirst {
		t.Fatalf("expected %q, got %q", expectedFirst, actualFirst)
	}
	if actualLast != expectedLast {
		t.Fatalf("expected %q, got %q", expectedLast, actualLast)
	}

	p2 := Supervisor{}
	_, err := db.Query(EMPLOYEE).
		Where(EMPLOYEE_C_ID.Matches(p.Id)).
		SelectTo(&p2)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	fn := FullNameVO{
		firstName: expectedFirst,
		lastName:  expectedLast,
	}

	if p2.FullName != fn {
		t.Fatalf("Expected %+v, got %+v", fn, p2.FullName)
	}
}

func getNames(db db.IDb, id int64) (string, string) {
	var first, last string
	db.Query(EMPLOYEE).
		Column(EMPLOYEE_C_FIRST_NAME).
		Column(EMPLOYEE_C_LAST_NAME).
		Where(EMPLOYEE_C_ID.Matches(id)).SelectInto(&first, &last)
	return first, last
}

func (tt Tester) RunEmbeddedPtr(t *testing.T) {
	db := tt.Tm.Store()
	// clear catalog
	if _, err := db.Delete(EMPLOYEE).Execute(); err != nil {
		t.Fatalf("%+v", err)
	}

	expectedFirst := "Paulo"
	expectedLast := "Pereira"
	p := Supervisor2{
		FullName: &FullNameVO{
			firstName: expectedFirst,
			lastName:  expectedLast,
		},
	}

	if _, err := db.Insert(EMPLOYEE).Submit(&p); err != nil {
		t.Fatalf("%+v", err)
	}
	actualFirst, actualLast := getNames(db, *p.Id)

	if actualFirst != expectedFirst {
		t.Fatalf("expected %q, got %q", expectedFirst, actualFirst)
	}
	if actualLast != expectedLast {
		t.Fatalf("expected %q, got %q", expectedLast, actualLast)
	}

	p2 := Supervisor2{}
	_, err := db.Query(EMPLOYEE).
		Where(EMPLOYEE_C_ID.Matches(p.Id)).
		SelectTo(&p2)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	fn := FullNameVO{
		firstName: expectedFirst,
		lastName:  expectedLast,
	}

	if p2.FullName == nil || *p2.FullName != fn {
		t.Fatalf("Expected %+v, got %+v", fn, p2.FullName)
	}
}
