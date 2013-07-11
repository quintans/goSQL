package db

import (
	"github.com/quintans/goSQL/dbx"
	"github.com/quintans/toolkit/log"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/db")

func init() {
	// activates output of program file line
	logger.CallDepth(1)
}

var OPTIMISTIC_LOCK_MSG = "No update was possible for this version of the data. Data may have changed."
var VERSION_SET_MSG = "Unable to set Version data."

type IDb interface {
	GetTranslator() Translator
	GetConnection() dbx.IConnection
	Query(table *Table) *Query
	Insert(table *Table) *Insert
	Delete(table *Table) *Delete
	Update(table *Table) *Update
}

var _ IDb = &Db{}

func NewDb(connection dbx.IConnection, translator Translator) *Db {
	return &Db{connection, translator}
}

type Db struct {
	Connection dbx.IConnection
	Translator Translator
}

func (this *Db) GetTranslator() Translator {
	return this.Translator
}

func (this *Db) GetConnection() dbx.IConnection {
	return this.Connection
}

// the idea is to centralize the query creation so that future customization could be made
func (this *Db) Query(table *Table) *Query {
	return NewQuery(this, table)
}

// the idea is to centralize the query creation so that future customization could be made
func (this *Db) Insert(table *Table) *Insert {
	return NewInsert(this, table)
}

// the idea is to centralize the query creation so that future customization could be made
func (this *Db) Delete(table *Table) *Delete {
	return NewDelete(this, table)
}

// the idea is to centralize the query creation so that future customization could be made
func (this *Db) Update(table *Table) *Update {
	return NewUpdate(this, table)
}
