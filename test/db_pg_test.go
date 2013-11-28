package test

import (
	. "github.com/quintans/goSQL/db"
	trx "github.com/quintans/goSQL/translators"

	_ "github.com/lib/pq"

	"database/sql"
	"fmt"
	"testing"
)

func InitPostgreSQL() (ITransactionManager, *sql.DB) {
	logger.Infof("******* Using PostgreSQL *******\n")

	RAW_SQL = "SELECT name FROM book WHERE name LIKE $1"

	translator := trx.NewPostgreSQLTranslator()
	translator.RegisterTranslation(
		TOKEN_SECONDSDIFF,
		func(dmlType DmlType, token Tokener, tx Translator) string {
			m := token.GetMembers()
			return fmt.Sprintf(
				"EXTRACT(EPOCH FROM (%s - %s))",
				tx.Translate(dmlType, m[0]),
				tx.Translate(dmlType, m[1]),
			)
		},
	)

	return InitDB("postgres", "dbname=gosql user=postgres password=postgres sslmode=disable", translator)
}

func TestPostgreSQL(t *testing.T) {
	tm, theDB := InitPostgreSQL()
	RunAll(tm, t)
	theDB.Close()
}
