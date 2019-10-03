package db

import (
	"github.com/quintans/goSQL/dbx"
	"github.com/quintans/toolkit/cache"

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
	With(db IDb) ITransactionManager
	Transaction(handler func(db IDb) error) error
	NoTransaction(handler func(db IDb) error) error
	Store() IDb
}

var _ ITransactionManager = (*TransactionManager)(nil)

type TransactionManager struct {
	database  *sql.DB
	dbFactory func(dbx.IConnection) IDb
	stmtCache *cache.LRUCache
}

// NewTransactionManager creates a new Transaction Manager
// database is the connection pool
// dbFactory is a database connection factory. This factory accepts boolean flag that indicates if the created IDb is still valid.
// This may be useful if an Entity holds a reference to the IDb to do lazy loading.
func NewTransactionManager(database *sql.DB, dbFactory func(dbx.IConnection) IDb, capacity int) *TransactionManager {
	this := new(TransactionManager)
	this.database = database
	this.dbFactory = dbFactory
	if capacity > 1 {
		this.stmtCache = cache.NewLRUCache(capacity)
	}
	return this
}

func (this *TransactionManager) With(db IDb) ITransactionManager {
	if db == nil {
		return this
	}
	return HollowTransactionManager{db}
}

func (this *TransactionManager) Transaction(handler func(db IDb) error) error {
	logger.Debugf("Transaction begin")
	tx, err := this.database.Begin()

	if err != nil {
		return err
	}
	defer func() {
		err := recover()
		if err != nil {
			logger.Debug("Transaction end in panic: ROLLBACK")
			tx.Rollback()
			panic(err) // up you go
		}
	}()

	var myTx = new(MyTx)
	myTx.Tx = tx
	myTx.stmtCache = this.stmtCache

	inTx := new(bool)
	*inTx = true
	err = handler(this.dbFactory(myTx))
	*inTx = false
	if err == nil {
		logger.Debug("Transaction end: COMMIT")
		tx.Commit()
	} else {
		logger.Debug("Transaction end: ROLLBACK")
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
	err := handler(this.dbFactory(myTx))
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
	return this.dbFactory(this.database)
}

var _ ITransactionManager = HollowTransactionManager{}

type HollowTransactionManager struct {
	db IDb
}

func (this HollowTransactionManager) With(db IDb) ITransactionManager {
	return HollowTransactionManager{db}
}

func (this HollowTransactionManager) Transaction(handler func(db IDb) error) error {
	return handler(this.db)
}

func (this HollowTransactionManager) NoTransaction(handler func(db IDb) error) error {
	return handler(this.db)
}

func (this HollowTransactionManager) Store() IDb {
	return this.db
}
