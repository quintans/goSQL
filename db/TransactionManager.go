package db

import (
	"database/sql"
	"github.com/quintans/goSQL/dbx"
	"github.com/quintans/toolkit/cache"
	"runtime/debug"
)

var _ dbx.IConnection = &MyTx{}

type MyTx struct {
	tx        *sql.Tx
	stmtCache *cache.LRUCache
}

func (this *MyTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return this.tx.Exec(query, args...)
}

// The implementor of Prepare should cache the prepared statements
func (this *MyTx) Prepare(query string) (*sql.Stmt, error) {
	var err error
	var stmt *sql.Stmt
	if this.stmtCache == nil {
		stmt, err = this.tx.Prepare(query)
	} else {
		s, _ := this.stmtCache.GetIfPresent(query)
		stmt, _ = s.(*sql.Stmt)
		if stmt == nil {
			stmt, err = this.tx.Prepare(query)
			if err == nil {
				this.stmtCache.Put(query, stmt)
			}
		} else {
			stmt = this.tx.Stmt(stmt)
		}
	}
	return stmt, err
}

func (this *MyTx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return this.tx.Query(query, args...)
}

func (this *MyTx) QueryRow(query string, args ...interface{}) *sql.Row {
	return this.tx.QueryRow(query, args...)
}

type ITransactionManager interface {
	Transaction(handler func(db IDb) error) error
	Store() IDb
}

var _ ITransactionManager = &TransactionManager{}

type TransactionManager struct {
	database  *sql.DB
	dbFactory func(c dbx.IConnection) IDb
	stmtCache *cache.LRUCache
}

func NewTransactionManager(database *sql.DB, dbFactory func(c dbx.IConnection) IDb, capacity int) *TransactionManager {
	this := new(TransactionManager)
	this.database = database
	this.dbFactory = dbFactory
	if capacity > 1 {
		this.stmtCache = cache.NewLRUCache(capacity)
	}
	return this
}

func (this *TransactionManager) Transaction(handler func(db IDb) error) error {
	tx, err := this.database.Begin()

	if err != nil {
		return err
	}
	defer func() {
		err := recover()
		if err != nil {
			logger.Fatalf("Transaction error: %s\n%s", err, debug.Stack())
			tx.Rollback()
			panic(err) // up you go
		}
	}()

	var myTx *MyTx
	if this.stmtCache == nil {
		myTx = &MyTx{tx, nil}
	} else {
		myTx = &MyTx{tx, this.stmtCache}
	}
	err = handler(this.dbFactory(myTx))
	if err == nil {
		tx.Commit()
	} else {
		logger.Debugf("%s", "ROLLBACK")
		tx.Rollback()
	}
	return err
}

/*
func (this TransactionManager) WithoutTransaction(handler func(db IDb) error) error {
	// TODO: use cache for the prepared statements
	return handler(this.dbFactory(this.database))
}
*/

func (this *TransactionManager) Store() IDb {
	return this.dbFactory(this.database)
}
