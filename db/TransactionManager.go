package db

import (
	"github.com/quintans/goSQL/dbx"
	"github.com/quintans/toolkit/cache"
	. "github.com/quintans/toolkit/ext"

	"database/sql"
	"runtime/debug"
)

var _ dbx.IConnection = &MyTx{}
var _ dbx.IConnection = &NoTx{}

type MyTx struct {
	*sql.Tx
	stmtCache *cache.LRUCache
}

// The implementor of Prepare should cache the prepared statements
func (this *MyTx) Prepare(query string) (*sql.Stmt, error) {
	var err error
	var stmt *sql.Stmt
	if this.stmtCache == nil {
		stmt, err = this.Tx.Prepare(query)
	} else {
		s, _ := this.stmtCache.GetIfPresent(query)
		stmt, _ = s.(*sql.Stmt)
		if stmt == nil {
			stmt, err = this.Tx.Prepare(query)
			if err == nil {
				this.stmtCache.Put(query, stmt)
			}
		} else {
			stmt = this.Tx.Stmt(stmt)
		}
	}
	return stmt, err
}

type NoTx struct {
	*sql.DB
	stmtCache *cache.LRUCache
}

// The implementor of Prepare should cache the prepared statements
func (this *NoTx) Prepare(query string) (*sql.Stmt, error) {
	// 6.12.2013
	// At the moment there is no way to reassign a statement to another connection,
	// so this code is commented
	/*
		var err error
		var stmt *sql.Stmt
		if this.stmtCache == nil {
			stmt, err = this.DB.Prepare(query)
		} else {
			s, _ := this.stmtCache.GetIfPresent(query)
			stmt, _ = s.(*sql.Stmt)
			if stmt == nil {
				stmt, err = this.DB.Prepare(query)
				if err == nil {
					this.stmtCache.Put(query, stmt)
				}
			} else {
				stmt = this.DB.Stmt(stmt)
			}
		}
		return stmt, err
	*/

	return this.DB.Prepare(query)
}

type ITransactionManager interface {
	Transaction(handler func(db IDb) error) error
	NoTransaction(handler func(db IDb) error) error
	Store() IDb
}

var _ ITransactionManager = &TransactionManager{}

type TransactionManager struct {
	database  *sql.DB
	dbFactory func(inTx *bool, c dbx.IConnection) IDb
	stmtCache *cache.LRUCache
}

func NewTransactionManager(database *sql.DB, dbFactory func(inTx *bool, c dbx.IConnection) IDb, capacity int) *TransactionManager {
	this := new(TransactionManager)
	this.database = database
	this.dbFactory = dbFactory
	if capacity > 1 {
		this.stmtCache = cache.NewLRUCache(capacity)
	}
	return this
}

func (this *TransactionManager) Transaction(handler func(db IDb) error) error {
	logger.Debugf("Transaction Begin")
	tx, err := this.database.Begin()

	if err != nil {
		return err
	}
	defer func() {
		err := recover()
		if err != nil {
			//logger.Fatalf("Transaction error: %s\n%s", err, debug.Stack())
			tx.Rollback()
			panic(err) // up you go
		}
	}()

	var myTx = new(MyTx)
	myTx.Tx = tx
	myTx.stmtCache = this.stmtCache

	inTx := new(bool)
	*inTx = true
	err = handler(this.dbFactory(inTx, myTx))
	*inTx = false
	if err == nil {
		logger.Debugf("%s", "COMMIT")
		tx.Commit()
	} else {
		logger.Debugf("%s", "ROLLBACK")
		tx.Rollback()
	}
	return err
}

func (this *TransactionManager) NoTransaction(handler func(db IDb) error) error {
	logger.Debugf("TransactionLESS Begin")
	defer func() {
		err := recover()
		if err != nil {
			logger.Fatalf("TransactionLESS error: %s\n%s", err, debug.Stack())
			panic(err) // up you go
		}
	}()

	var myTx = new(NoTx)
	myTx.DB = this.database
	myTx.stmtCache = this.stmtCache

	inTx := new(bool)
	*inTx = true
	err := handler(this.dbFactory(inTx, myTx))
	*inTx = false
	logger.Debugf("TransactionLESS End")
	return err
}

/*
func (this TransactionManager) WithoutTransaction(handler func(db IDb) error) error {
	// TODO: use cache for the prepared statements
	return handler(this.dbFactory(this.database))
}
*/

func (this *TransactionManager) Store() IDb {
	return this.dbFactory(BoolPtr(false), this.database)
}
