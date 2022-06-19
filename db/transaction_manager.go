package db

import (
	"database/sql"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/quintans/faults"
	"github.com/quintans/goSQL/dbx"
)

var (
	_ dbx.IConnection = &MyTx{}
	_ dbx.IConnection = &NoTx{}
)

type MyTx struct {
	*sql.Tx
}

type NoTx struct {
	*sql.DB
}

type ITransactionManager interface {
	With(db IDb) ITransactionManager
	Transaction(handler func(db IDb) error) error
	NoTransaction(handler func(db IDb) error) error
	Store() IDb
}

var _ ITransactionManager = (*TransactionManager)(nil)

type TransactionManager struct {
	cache      sync.Map
	database   *sql.DB
	translator Translator
	dbFactory  func(dbx.IConnection, Mapper) IDb
}

func TmWithDbFactory(dbFactory func(dbx.IConnection, Mapper) IDb) func(*TransactionManager) {
	return func(t *TransactionManager) {
		t.dbFactory = dbFactory
	}
}

// NewTransactionManager creates a new Transaction Manager
func NewTransactionManager(database *sql.DB, translator Translator, options ...func(*TransactionManager)) *TransactionManager {
	t := &TransactionManager{
		database:   database,
		translator: translator,
		dbFactory: func(c dbx.IConnection, cache Mapper) IDb {
			return NewDb(c, translator, cache)
		},
	}

	for _, o := range options {
		o(t)
	}
	return t
}

func (d *TransactionManager) RegisterType(v interface{}) error {
	_, err := d.Mappings(reflect.TypeOf(v))
	return faults.Wrap(err)
}

func (t *TransactionManager) Mappings(typ reflect.Type) (map[string]*StructProperty, error) {
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	elem, ok := t.cache.Load(typ)
	if !ok {
		// create an attribute data structure as a map of types keyed by a string.
		attrs := map[string]*StructProperty{}

		err := t.walkTreeStruct(typ, attrs, nil)
		if err != nil {
			return nil, faults.Wrap(err)
		}

		elem, _ = t.cache.LoadOrStore(typ, attrs)
	}
	return elem.(map[string]*StructProperty), nil
}

func (t *TransactionManager) walkTreeStruct(typ reflect.Type, attrs map[string]*StructProperty, index []int) error {
	// if a pointer to a struct is passed, get the type of the dereferenced object
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	// Only structs are supported so return an empty result if the passed object
	// isn't a struct
	if typ.Kind() != reflect.Struct {
		return nil
	}

	// loop through the struct's fields and set the map
	num := typ.NumField()
	for i := 0; i < num; i++ {
		p := typ.Field(i)
		var omit, embeded bool
		var converter Converter
		sqlVal := p.Tag.Get(sqlKey)
		if sqlVal != "" {
			splits := strings.Split(sqlVal, ",")
			for _, s := range splits {
				v := strings.TrimSpace(s)
				switch v {
				case sqlOmissionVal:
					omit = true
				case sqlEmbeddedVal:
					embeded = true
				default:
					if strings.HasPrefix(v, converterTag) {
						cn := v[len(converterTag):]
						converter = t.translator.GetConverter(cn)
						if converter == nil {
							return faults.Errorf("Converter %s is not registered", cn)
						}
					}
				}
			}
		}
		x := append(index, i)
		if p.Anonymous {
			if err := t.walkTreeStruct(p.Type, attrs, x); err != nil {
				return faults.Wrap(err)
			}
		} else if embeded {
			if err := t.walkTreeStruct(p.Type, attrs, x); err != nil {
				return faults.Wrap(err)
			}
		} else {
			ep := &StructProperty{}
			ep.getter = makeGetter(x)
			ep.setter = makeSetter(x)
			capitalized := strings.ToUpper(p.Name[:1]) + p.Name[1:]
			attrs[capitalized] = ep
			ep.Omit = omit
			ep.converter = converter
			// we want pointers. only pointer are addressable
			if p.Type.Kind() == reflect.Ptr || p.Type.Kind() == reflect.Slice || p.Type.Kind() == reflect.Array {
				ep.Type = p.Type
			} else {
				ep.Type = reflect.PtrTo(p.Type)
			}

			if p.Type.Kind() == reflect.Slice || p.Type.Kind() == reflect.Array {
				ep.InnerType = p.Type.Elem()
			}
		}
	}
	return nil
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
		return faults.Wrap(err)
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

	inTx := new(bool)
	*inTx = true
	err = handler(t.dbFactory(myTx, t))
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
	return faults.Wrap(err)
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

	inTx := new(bool)
	*inTx = true
	err := handler(t.dbFactory(myTx, t))
	*inTx = false
	logger.Debugf("TransactionLESS End")
	return faults.Wrap(err)
}

/*
func (this TransactionManager) WithoutTransaction(handler func(db IDb) error) error {
	// TODO: use cache for the prepared statements
	return handler(this.dbFactory(this.database))
}
*/

func (t *TransactionManager) Store() IDb {
	return t.dbFactory(t.database, t)
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
