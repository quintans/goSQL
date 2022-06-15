package db

import (
	"database/sql"
	"runtime/debug"
	"sync"

	"github.com/quintans/goSQL/dbx"
	"github.com/quintans/toolkit/cache"
)

var (
	_ dbx.IConnection = &MyTx{}
	_ dbx.IConnection = &NoTx{}
)

type MyTx struct {
	*sql.Tx
	stmtCache *cache.LRUCache
}

// The implementor of Prepare should cache the prepared statements
func (t *MyTx) Prepare(query string) (*sql.Stmt, error) {
	var err error
	var stmt *sql.Stmt
	if t.stmtCache == nil {
		stmt, err = t.Tx.Prepare(query)
	} else {
		s, _ := t.stmtCache.GetIfPresent(query)
		stmt, _ = s.(*sql.Stmt)
		if stmt == nil {
			stmt, err = t.Tx.Prepare(query)
			if err == nil {
				t.stmtCache.Put(query, stmt)
			}
		} else {
			stmt = t.Tx.Stmt(stmt)
		}
	}
	return stmt, err
}

type NoTx struct {
	*sql.DB
	stmtCache *cache.LRUCache
}

// The implementor of Prepare should cache the prepared statements
func (t *NoTx) Prepare(query string) (*sql.Stmt, error) {
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

	return t.DB.Prepare(query)
}

type ITransactionManager interface {
	With(db IDb) ITransactionManager
	Transaction(handler func(db IDb) error) error
	NoTransaction(handler func(db IDb) error) error
	Store() IDb
}

var _ ITransactionManager = (*TransactionManager)(nil)

type TransactionManager struct {
	cache     sync.Map
	database  *sql.DB
	dbFactory func(dbx.IConnection, *sync.Map) IDb
	stmtCache *cache.LRUCache
}

// NewTransactionManager creates a new Transaction Manager
// database is the connection pool
// dbFactory is a database connection factory. This factory accepts boolean flag that indicates if the created IDb is still valid.
// This may be useful if an Entity holds a reference to the IDb to do lazy loading.
func NewTransactionManager(database *sql.DB, dbFactory func(dbx.IConnection, *sync.Map) IDb, capacity int) *TransactionManager {
	t := new(TransactionManager)
	t.database = database
	t.dbFactory = dbFactory
	if capacity > 1 {
		t.stmtCache = cache.NewLRUCache(capacity)
	}
	return t
}

func NewDefaultTransactionManager(database *sql.DB, translator Translator) *TransactionManager {
	// transaction manager
	return NewTransactionManager(
		database,
		func(c dbx.IConnection, cache *sync.Map) IDb {
			return NewDb(c, translator, cache)
		},
		1000,
	)
}

func (t *TransactionManager) SetCacheSize(capacity int) {
	if capacity > 1 {
		t.stmtCache = cache.NewLRUCache(capacity)
	}
}

func (t *TransactionManager) SetDbFactory(dbFactory func(dbx.IConnection, *sync.Map) IDb) {
	t.dbFactory = dbFactory
}

func (t *TransactionManager) With(db IDb) ITransactionManager {
	if db == nil {
		return t
	}
	return HollowTransactionManager{db}
}

func (t *TransactionManager) Transaction(handler func(db IDb) error) error {
	logger.Debugf("Transaction begin")
	tx, err := t.database.Begin()
	if err != nil {
		return err
	}
	defer func() {
		err := recover()
		if err != nil {
			logger.Errorf("Transaction end in panic: %v", err)
			rerr := tx.Rollback()
			if rerr != nil {
				logger.Errorf("failed to rollback: %v", rerr)
			}
			panic(err) // up you go
		}
	}()

	myTx := new(MyTx)
	myTx.Tx = tx
	myTx.stmtCache = t.stmtCache

	inTx := new(bool)
	*inTx = true
	err = handler(t.dbFactory(myTx, &t.cache))
	*inTx = false
	if err == nil {
		logger.Debug("Transaction end: COMMIT")
		cerr := tx.Commit()
		if cerr != nil {
			logger.Errorf("failed to commit: %v", cerr)
		}
	} else {
		logger.Debug("Transaction end: ROLLBACK")
		rerr := tx.Rollback()
		if rerr != nil {
			logger.Errorf("failed to rollback: %v", rerr)
		}
	}
	return err
}

func (t *TransactionManager) NoTransaction(handler func(db IDb) error) error {
	logger.Debugf("TransactionLESS Begin")
	defer func() {
		err := recover()
		if err != nil {
			logger.Fatalf("TransactionLESS error: %s\n%s", err, debug.Stack())
			panic(err) // up you go
		}
	}()

	myTx := new(NoTx)
	myTx.DB = t.database
	myTx.stmtCache = t.stmtCache

	inTx := new(bool)
	*inTx = true
	err := handler(t.dbFactory(myTx, &t.cache))
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

func (t *TransactionManager) Store() IDb {
	return t.dbFactory(t.database, &t.cache)
}

var _ ITransactionManager = HollowTransactionManager{}

type HollowTransactionManager struct {
	db IDb
}

func (t HollowTransactionManager) With(db IDb) ITransactionManager {
	return HollowTransactionManager{db}
}

func (t HollowTransactionManager) Transaction(handler func(db IDb) error) error {
	return handler(t.db)
}

func (t HollowTransactionManager) NoTransaction(handler func(db IDb) error) error {
	return handler(t.db)
}

func (t HollowTransactionManager) Store() IDb {
	return t.db
}
