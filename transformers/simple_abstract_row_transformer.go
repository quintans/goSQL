package transformers

import (
	"database/sql"

	"github.com/quintans/goSQL/dbx"
	tk "github.com/quintans/toolkit"
	coll "github.com/quintans/toolkit/collections"
)

type SimpleAbstractRowTransformer struct {
	Transformer func(rows *sql.Rows) (interface{}, error)
}

var _ dbx.IRowTransformer = &SimpleAbstractRowTransformer{}

func (s *SimpleAbstractRowTransformer) BeforeAll() coll.Collection {
	return coll.NewArrayList()
}

func (s *SimpleAbstractRowTransformer) Transform(rows *sql.Rows) (interface{}, error) {
	if s.Transformer != nil {
		return s.Transformer(rows)
	}
	return nil, &tk.Fail{Code: dbx.FAULT_PARSE_STATEMENT, Message: "Undefined Transformer function"}
}

func (s *SimpleAbstractRowTransformer) OnTransformation(result coll.Collection, instance interface{}) {
	if instance != nil {
		result.Add(instance)
	}
}

func (s *SimpleAbstractRowTransformer) AfterAll(result coll.Collection) {
}
