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

func (this *SimpleAbstractRowTransformer) BeforeAll() coll.Collection {
	return coll.NewArrayList()
}

func (this *SimpleAbstractRowTransformer) Transform(rows *sql.Rows) (interface{}, error) {
	if this.Transformer != nil {
		return this.Transformer(rows)
	}
	return nil, &tk.Fail{dbx.FAULT_PARSE_STATEMENT, "Undefined Transformer function"}
}

func (this *SimpleAbstractRowTransformer) OnTransformation(result coll.Collection, instance interface{}) {
	if instance != nil {
		result.Add(instance)
	}
}

func (this *SimpleAbstractRowTransformer) AfterAll(result coll.Collection) {
}
