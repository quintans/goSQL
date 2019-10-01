package postgresql

import (
	"database/sql"
	"fmt"
	"testing"

	. "github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/test/common"
	trx "github.com/quintans/goSQL/translators"
	"github.com/quintans/toolkit/log"

	_ "github.com/lib/pq"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/test")

func TestPostgreSQL(t *testing.T) {
	logger.Infof("******* Using PostgreSQL *******\n")

	expPort := "5432/tcp"
	ctx, server, port, err := common.Container(
		"postgres:9.6.8",
		expPort,
		map[string]string{"POSTGRES_PASSWORD": "secret"},
		"postgres",
		"dbname=postgres user=postgres password=secret port=<port> sslmode=disable",
		1,
	)

	defer server.Terminate(ctx)
	if err != nil {
		t.Error(err)
	}

	tm, theDB := InitPostgreSQL(t, port.Port())
	tester := common.Tester{DbName: common.Postgres}
	tester.RunAll(tm, t)
	theDB.Close()
}

func InitPostgreSQL(t *testing.T, port string) (ITransactionManager, *sql.DB) {
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

	return common.InitDB(
		t,
		"postgres",
		fmt.Sprintf("dbname=postgres user=postgres password=secret port=%s sslmode=disable", port),
		translator,
		"tables_postgresql.sql",
	)
}
