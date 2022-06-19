package dbx

import (
	"context"
	"database/sql"

	coll "github.com/quintans/toolkit/collections"
)

type IConnection interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type IRowTransformer interface {
	// Initializes the collection that will hold the results
	// return Creates a Collection
	BeforeAll() coll.Collection

	Transform(rows *sql.Rows) (interface{}, error)

	// Executes additional decision/action over the transformed object.<br>
	// For example, It can decide not to include if the result is repeated...
	//
	// param object: The transformed instance
	OnTransformation(result coll.Collection, instance interface{})

	AfterAll(result coll.Collection)
}
