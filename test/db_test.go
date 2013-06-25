package test

import (
	"github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/dbx"
	trx "github.com/quintans/goSQL/translators"
	"github.com/quintans/toolkit/ext"
	"github.com/quintans/toolkit/log"

	_ "github.com/go-sql-driver/mysql"

	"database/sql"
	"testing"
)

var TM db.ITransactionManager

func init() {
	log.Register("/", log.INFO, log.NewConsoleAppender(false))

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

	TM = db.NewTransactionManager(
		// database
		mydb,
		// databse context factory
		func(c dbx.IConnection) db.IDb {
			//return db.NewDb(c, trx.NewFirebirdSQLTranslator())
			return db.NewDb(c, trx.NewMySQL5Translator())
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

func resetDB() {
	if err := TM.Transaction(func(DB db.IDb) error {
		var err error
		// clear publisers
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

		return nil
	}); err != nil {
		panic(err)
	}
}

func TestSelectUTF8(t *testing.T) {
	resetDB()

	// get the databse context
	store := TM.Store()
	// the target entity
	var publisher = Publisher{}

	ok, err := store.Query(PUBLISHER).
		All().
		Where(PUBLISHER_C_ID.Matches(2)).
		SelectTo(&publisher)

	if err != nil {
		t.Errorf("%s", err)
	} else if !ok || *publisher.Id != 2 || *publisher.Version != 1 || *publisher.Name != PUBLISHER_UTF8_NAME {
		t.Errorf("The record for publisher id 2, was not properly retrived. Retrived %s", publisher)
	}
}

func TestInsertReturningKey(t *testing.T) {
	resetDB()

	var err error
	if err = TM.Transaction(func(store db.IDb) error {
		key, err := store.Insert(PUBLISHER).
			Columns(PUBLISHER_C_ID, PUBLISHER_C_VERSION, PUBLISHER_C_NAME).
			Values(nil, 1, "New Editions").
			Execute()
		if err != nil {
			return err
		}

		if key == 0 {
			t.Error("The Auto Insert Key for a null ID column was not retrived")
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
			t.Error("The Auto Insert Key for a absent ID column was not retrived")
		}

		return nil
	}); err != nil {
		t.Errorf("Failed Insert Returning Key: %s", err)
	}
}

func TestInsertStructReturningKey(t *testing.T) {
	resetDB()

	var err error
	if err = TM.Transaction(func(store db.IDb) error {
		var pub Publisher
		pub.Name = ext.StrPtr("Untited Editors")
		key, err := store.Insert(PUBLISHER).Submit(pub)
		if err != nil {
			return err
		}

		if key == 0 {
			t.Error("The Auto Insert Key for the ID column was not retrived")
		}

		var pubPtr = new(Publisher)
		pubPtr.Name = ext.StrPtr("Untited Editors")
		key, err = store.Insert(PUBLISHER).Submit(pubPtr)
		if err != nil {
			return err
		}

		if key == 0 {
			t.Error("The Auto Insert Key for the ID column was not retrived")
		}

		return nil
	}); err != nil {
		t.Errorf("Failed Struct Insert Return Key: %s", err)
	}
}

func TestUpdate(t *testing.T) {
	resetDB()

	var err error
	if err = TM.Transaction(func(store db.IDb) error {
		affectedRows, err := store.Update(PUBLISHER).
			Set(PUBLISHER_C_NAME, "Dummy"). // column to update
			Set(PUBLISHER_C_VERSION, 2).    // increment version
			Where(
			PUBLISHER_C_ID.Matches(1),
			PUBLISHER_C_VERSION.Matches(1),
		).
			Execute()
		if err != nil {
			return err
		}

		if affectedRows != 1 {
			t.Error("The record was not updated")
		}

		return nil
	}); err != nil {
		t.Errorf("Failed Update Test: %s", err)
	}
}

//func Test(t *testing.T) {
//	resetDB()

//	var err error
//	if err = TM.Transaction(func(store db.IDb) error {

//		return nil
//	}); err != nil {
//		t.Errorf("Failed ... Test: %s", err)
//	}
//}
