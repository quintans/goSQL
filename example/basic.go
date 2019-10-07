package main

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
	Id      int64
	Version int64
	Name    string
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
	mydb, err := sql.Open("mysql", "root:root@/gosql?parseTime=true")
	if err != nil {
		panic(err)
	}

	translator := trx.NewMySQL5Translator()

	// transaction manager
	TM = NewTransactionManager(
		// database
		mydb,
		// database context factory
		func(c dbx.IConnection) IDb {
			return NewDb(c, translator)
		},
		// statement cache
		1000,
	)
}

func main() {
	// get the databse context
	store := TM.Store()
	// the target entity
	var publisher Publisher
	// Retrive
	_, err := store.Retrive(&publisher, 2)
	if err != nil {
		panic(err)
	}

	fmt.Println(publisher)
}
