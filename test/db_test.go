package test

import (
	"github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/dbx"
	trx "github.com/quintans/goSQL/translators"

	_ "github.com/go-sql-driver/mysql"

	"database/sql"
	"testing"
)

var TM db.ITransactionManager

func init() {
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

//func TestSelectUTF8(t *testing.T) {
//	var err error
//	if err = TM.Transaction(func(store db.IDb) error {
//		if err := startup(store); err != nil {
//			return err
//		}

//		var publisher = Publisher{}
//		ok, err := store.Query(PUBLISHER).
//			All().
//			Where(PUBLISHER_C_ID.Matches(2)).
//			SelectTo(&publisher)
//		if err != nil {
//			return err
//		}

//		if !ok || *publisher.Id != 2 || *publisher.Version != 1 || *publisher.Name != "Edições Lusas" {
//			t.Errorf("The record for publisher id 2, was not properly retrived. Retrived %s", publisher)
//		}

//		return nil
//	}); err != nil {
//		t.Errorf("Failed Select UTF8 Test: %s", err)
//	}
//}
