package dbx

import (
	"database/sql"
	"reflect"

	"github.com/quintans/faults"
	tk "github.com/quintans/toolkit"
	coll "github.com/quintans/toolkit/collections"
	"github.com/quintans/toolkit/log"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/dbx")

// Class that simplifies the execution o Database Access
type SimpleDBA struct {
	// The connection to execute the query in.
	connection IConnection
}

func NewSimpleDBA(connection IConnection) *SimpleDBA {
	this := new(SimpleDBA)
	this.connection = connection
	return this
}

// Execute an SQL SELECT with named replacement parameters.<br>
// The caller is responsible for closing the connection.
//
// param sql: The query to execute.
// param params: The replacement parameters.
// param rt: The handler that converts the results into an object.
// return The Collection returned by the handler and a Fail if a database access error occurs
func (s *SimpleDBA) QueryCollection(
	sql string,
	rt IRowTransformer,
	params ...interface{},
) (coll.Collection, error) {
	rows, err := s.connection.Query(sql, params...)
	if err != nil {
		return nil, rethrow(err, "executing query collection", sql, params...)
	}
	defer rows.Close()

	result := rt.BeforeAll()

	for rows.Next() {
		instance, err := rt.Transform(rows)
		if err != nil {
			return nil, rethrow(err, "query collection transform", sql, params...)
		}
		rt.OnTransformation(result, instance)
	}

	if err = rows.Err(); err != nil {
		return nil, rethrow(err, "closing rows for query collection", sql, params...)
	}

	rt.AfterAll(result)

	return result, nil
}

func (s *SimpleDBA) Query(
	sql string,
	transformer func(rows *sql.Rows) (interface{}, error),
	params ...interface{},
) ([]interface{}, error) {
	rows, err := s.connection.Query(sql, params...)
	if err != nil {
		return nil, rethrow(err, "executing query", sql, params...)
	}
	defer rows.Close()

	results := make([]interface{}, 0, 10)
	for rows.Next() {
		result, err := transformer(rows)
		if err != nil {
			return nil, rethrow(err, "query transform", sql, params...)
		}
		results = append(results, result)
	}

	if err = rows.Err(); err != nil {
		return nil, faults.Errorf("closing rows for query: %w", faults.Wrap(err))
	}

	return results, nil
}

// the transformer will be responsible for creating  the result list
func (s *SimpleDBA) QueryClosure(
	query string,
	transformer func(rows *sql.Rows) error,
	params ...interface{},
) error {
	rows, err := s.connection.Query(query, params...)
	if err != nil {
		return rethrow(err, "executing query closure", query, params...)
	}
	defer rows.Close()

	for rows.Next() {
		err := transformer(rows)
		if err != nil {
			return rethrow(err, "query closure transform", query, params...)
		}
	}

	if err = rows.Err(); err != nil {
		return faults.Errorf("closing rows for query closure: %w", faults.Wrap(err))
	}

	return nil
}

//List using the closure arguments.
//A function is used to build the result list.
//The types for scanning are supplied by the function arguments. Arguments can be pointers or not.
//Reflection is used to determine the arguments types.
//
//ex:
//  roles = make([]string, 0)
//  var role string
//  q.QueryInto(func(role *string) {
//	  roles = append(roles, *role)
//  })
func (s *SimpleDBA) QueryInto(
	query string,
	closure interface{},
	params ...interface{},
) ([]interface{}, error) {
	// determine types and instanciate them
	ftype := reflect.TypeOf(closure)
	if ftype.Kind() != reflect.Func {
		return nil, faults.Errorf("expected a function with the signature func(primitive1, ..., primitiveN) [anything], but got %s", ftype.String())
	}

	size := ftype.NumIn() // number of input variables
	instances := make([]interface{}, size)
	targets := make([]reflect.Type, size)
	for i := 0; i < size; i++ {
		arg := ftype.In(i) // type of input variable i
		targets[i] = arg   // collects the target types
		// the scan elements must be all pointers
		if arg.Kind() == reflect.Ptr {
			// Instanciates a pointer. Interface() returns the pointer instance.
			instances[i] = reflect.New(arg).Interface()
		} else {
			// creates a pointer of the type of the zero type
			instances[i] = reflect.New(reflect.PtrTo(arg)).Interface()
		}
	}

	var results []interface{}
	// output must be at most 1
	if ftype.NumOut() > 1 {
		return nil, faults.Errorf("a function must have at most one output, but got %d outputs", ftype.NumOut())
	} else if ftype.NumOut() == 1 {
		results = make([]interface{}, 0)
	}

	err := s.QueryClosure(query, func(rows *sql.Rows) error {
		err := rows.Scan(instances...)
		if err != nil {
			return faults.Wrap(err)
		}
		values := make([]reflect.Value, size)
		for k, v := range instances {
			// Elem() gets the underlying object of the interface{}
			e := reflect.ValueOf(v).Elem()
			if targets[k].Kind() == reflect.Ptr {
				// if pointer type use directly
				values[k] = e
			} else {
				if e.IsNil() {
					// was nil, so we must create its zero value
					values[k] = reflect.Zero(targets[k])
				} else {
					// use underlying value of the pointer
					values[k] = e.Elem()
				}
			}
		}
		res := reflect.ValueOf(closure).Call(values)
		if results != nil { // expects result. ftype.NumOut() == 1
			results = append(results, res[0].Interface())
		}
		return nil
	}, params...)

	return results, faults.Wrap(err)
}

// Execute an SQL SELECT query with named parameters returning the first result.
//
// param <T>
//            the result object type
// param conn
//            The connection to execute the query in.
// param sql
//            The query to execute.
// param rt
//            The handler that converts the results into an object.
// param params
//            The named parameters.
// @return The transformed result
func (s *SimpleDBA) QueryFirst(
	sql string,
	params map[string]interface{},
	rt IRowTransformer,
) (interface{}, error) {
	result, fail1 := s.QueryCollection(sql, rt, params)
	if fail1 != nil {
		return nil, fail1
	}

	if result.Size() > 0 {
		return result.Enumerator().Next(), nil
	}
	return nil, nil
}

// Execute an SQL SELECT query with named parameters returning the first result.
//
// param conn
//            The connection to execute the query in.
// param sql
//            The query to execute.
// param params
//            The named parameters.
// @return if there was a row scan and error
func (s *SimpleDBA) QueryRow(
	query string,
	params []interface{},
	dest ...interface{},
) (bool, error) {
	err := s.connection.QueryRow(query, params...).Scan(dest...)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, rethrow(err, "executing query row", query, params...)
	}

	return true, nil
}

////////////////////////////////////////////////////////////////////////

///**
// Execute an SQL INSERT, UPDATE, or DELETE query.
//
// param conn
//            The connection to use to run the query.
// param sql
//            The SQL to execute.
// param params
//            The query named parameters.
// @return The number of rows affected.
// */
func (s *SimpleDBA) Update(sql string, params ...interface{}) (int64, error) {
	result, err := s.connection.Exec(sql, params...)
	if err != nil {
		return 0, rethrow(err, "update", sql, params...)
	}
	return result.RowsAffected()
}

func (s *SimpleDBA) Delete(sql string, params ...interface{}) (int64, error) {
	return s.Update(sql, params...)
}

func (s *SimpleDBA) Insert(sql string, params ...interface{}) (int64, error) {
	_, err := s.connection.Exec(sql, params...)
	if err != nil {
		return 0, rethrow(err, "executing", sql, params...)
	}

	// not supported in all drivers (ex: pq)
	// return result.LastInsertId()
	return 0, nil
}

func (s *SimpleDBA) InsertReturning(sql string, params ...interface{}) (int64, error) {
	var id int64
	_, err := s.QueryRow(sql, params, &id)
	if err != nil {
		return 0, faults.Wrap(err)
	}
	return id, nil
}

// Throws a new exception with a more informative error message.
//
// param cause
//            The original exception that will be chained to the new
//            exception when it's rethrown.
//
// param sql
//            The query that was executing when the exception happened.
//
// param params
//            The query replacement parameters; <code>nil</code> is a
//            valid value to pass in.

func rethrow(cause error, what string, sql string, params ...interface{}) error {
	msg := tk.NewStrBuffer()
	msg.Add(what).Add("\nSQL: ", sql, "\nParameters: ")
	if params != nil {
		msg.Addf("%v", params)
	}

	return faults.Errorf("%s: %w", msg.String(), cause)
}
