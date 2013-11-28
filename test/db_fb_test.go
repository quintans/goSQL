package test

import (
	. "github.com/quintans/goSQL/db"
	trx "github.com/quintans/goSQL/translators"

	_ "bitbucket.org/miquella/mgodbc" // float64 was fixed acording to issue #5

	"database/sql"
	"fmt"
	"testing"
)

func InitFirebirdSQL() (ITransactionManager, *sql.DB) {
	logger.Infof("******* Using FirebirdSQL *******\n")

	RAW_SQL = "SELECT name FROM book WHERE name LIKE ?"

	translator := trx.NewFirebirdSQLTranslator()
	translator.RegisterTranslation(
		TOKEN_SECONDSDIFF,
		func(dmlType DmlType, token Tokener, tx Translator) string {
			m := token.GetMembers()
			return fmt.Sprintf(
				"DATEDIFF(SECOND, %s, %s)",
				tx.Translate(dmlType, m[1]),
				tx.Translate(dmlType, m[0]),
			)
		},
	)

	return InitDB("mgodbc", "dsn=FbGoSQL;uid=SYSDBA;pwd=masterkey", translator)
}

func TestFirebirdSQL(t *testing.T) {
	tm, theDB := InitFirebirdSQL()
	RunAll(tm, t)
	theDB.Close()
}
