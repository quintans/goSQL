package db

//import (
//	tk "github.com/quintans/toolkit"
//)

type DmlType int

const (
	INSERT DmlType = iota
	UPDATE
	DELETE
	QUERY
)

// Converter converts the data stored in the database into the data stored in the struct and vice-versa
type Converter interface {
	ToDb(in interface{}) (interface{}, error)
	FromDbInstance() interface{}
	FromDb(in interface{}) (interface{}, error)
}

type Translator interface {
	GetPlaceholder(index int, name string) string
	// INSERT
	GetAutoKeyStrategy() AutoKeyStrategy
	GetSqlForInsert(insert *Insert) string
	// QUERY
	GetSqlForQuery(query *Query) string
	// UPDATE
	GetSqlForUpdate(update *Update) string
	// DELTE
	GetSqlForDelete(del *Delete) string
	// GetSqlForSequence(sequence *Sequence, nextValue bool) string
	GetAutoNumberQuery(column *Column) string
	//	GetMaxTableChars() int
	PaginateSQL(query *Query, sql string) string
	Translate(dmlType DmlType, token Tokener) string
	TableName(table *Table) string
	ColumnName(column *Column) string
	ColumnAlias(token Tokener, position int) string
	IgnoreNullKeys() bool
	RegisterConverter(name string, c Converter)
	GetConverter(name string) Converter
}
