package postgresql

import (
	. "github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/test/common"
	trx "github.com/quintans/goSQL/translators"
	"github.com/quintans/toolkit/log"

	_ "github.com/lib/pq"

	"database/sql"
	"fmt"
	"testing"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/test")

func InitPostgreSQL() (ITransactionManager, *sql.DB) {
	logger.Infof("******* Using PostgreSQL *******\n")

	common.RAW_SQL = "SELECT name FROM book WHERE name LIKE $1"

	translator := trx.NewPostgreSQLTranslator()
	translator.RegisterTranslation(
		common.TOKEN_SECONDSDIFF,
		func(dmlType DmlType, token Tokener, tx Translator) string {
			m := token.GetMembers()
			return fmt.Sprintf(
				"EXTRACT(EPOCH FROM (%s - %s))",
				tx.Translate(dmlType, m[0]),
				tx.Translate(dmlType, m[1]),
			)
		},
	)

	return common.InitDB("postgres", "dbname=gosql user=postgres password=postgres sslmode=disable", translator)
}

func TestPostgreSQL(t *testing.T) {
	tm, theDB := InitPostgreSQL()
	common.RunAll(tm, t)
	theDB.Close()
}
