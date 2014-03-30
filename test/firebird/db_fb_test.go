package firebird

import (
	. "github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/test/common"
	trx "github.com/quintans/goSQL/translators"
	"github.com/quintans/toolkit/log"

	_ "bitbucket.org/miquella/mgodbc" // float64 was fixed acording to issue #5

	"database/sql"
	"fmt"
	"testing"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/test")

func InitFirebirdSQL() (ITransactionManager, *sql.DB) {
	logger.Infof("******* Using FirebirdSQL *******\n")

	common.RAW_SQL = "SELECT name FROM book WHERE name LIKE ?"

	translator := trx.NewFirebirdSQLTranslator()
	translator.RegisterTranslation(
		common.TOKEN_SECONDSDIFF,
		func(dmlType DmlType, token Tokener, tx Translator) string {
			m := token.GetMembers()
			return fmt.Sprintf(
				"DATEDIFF(SECOND, %s, %s)",
				tx.Translate(dmlType, m[1]),
				tx.Translate(dmlType, m[0]),
			)
		},
	)

	return common.InitDB("mgodbc", "dsn=FbGoSQL;uid=SYSDBA;pwd=masterkey", translator)
}

func TestFirebirdSQL(t *testing.T) {
	tm, theDB := InitFirebirdSQL()
	common.RunAll(tm, t)
	theDB.Close()
}
