package main

import (
	"github.com/quintans/goSQL/db"
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
	PUBLISHER           = db.TABLE("PUBLISHER")
	PUBLISHER_C_ID      = PUBLISHER.KEY("ID")          // implicit map to field Id
	PUBLISHER_C_VERSION = PUBLISHER.VERSION("VERSION") // implicit map to field Version
	PUBLISHER_C_NAME    = PUBLISHER.COLUMN("NAME")     // implicit map to field Name
)

// the transaction manager
var tm db.ITransactionManager

func main() {
	// database configuration
	mydb, err := sql.Open("mysql", "root:root@/gosql?parseTime=true")
	if err != nil {
		fmt.Printf("%+v\n", err)
		panic(err)
	}

	// transaction manager
	tm = db.NewTransactionManager(mydb, trx.NewMySQL5Translator())

	// get the databse context
	store := tm.Store()
	// the target entity
	var publisher Publisher
	// Retrieve
	_, err = store.Retrieve(&publisher, 2)
	if err != nil {
		fmt.Printf("%+v\n", err)
		panic(err)
	}

	fmt.Println(publisher)
}
