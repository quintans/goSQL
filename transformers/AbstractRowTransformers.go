package transformers

import (
	"database/sql"
	"github.com/quintans/goSQL/api"
	"github.com/quintans/goSQL/dbx"
	tk "github.com/quintans/toolkit"
)

type SimpleAbstractRowTransformer struct {
	Transformer func(rows *sql.Rows) (tk.Hasher, error)
}

var _ api.IRowTransformer = &SimpleAbstractRowTransformer{}

func (this *SimpleAbstractRowTransformer) BeforeAll() coll.Collection {
	return tk.NewArrayList()
}

func (this *SimpleAbstractRowTransformer) Transform(rows *sql.Rows) (tk.Hasher, error) {
	if this.Transformer != nil {
		return this.Transformer(rows)
	}
	return nil, &tk.Fault{dbx.FAIL_STATEMENT, "Undefined Transformer function"}
}

func (this *SimpleAbstractRowTransformer) OnTransformation(result coll.Collection, instance tk.Hasher) {
	if instance != nil {
		result.Add(instance)
	}
}

func (this *SimpleAbstractRowTransformer) AfterAll(result coll.Collection) {
}
