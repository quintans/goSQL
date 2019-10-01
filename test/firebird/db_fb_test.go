package firebird

import (
	. "github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/test/common"
	trx "github.com/quintans/goSQL/translators"
	"github.com/quintans/toolkit/log"

	_ "github.com/nakagami/firebirdsql" // float64 was fixed acording to issue #5

	"database/sql"
	"fmt"
	"testing"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/test")

func TestFirebirdSQL(t *testing.T) {
	logger.Infof("******* Using FirebirdSQL *******\n")

	expPort := "3050/tcp"
	ctx, server, port, err := common.Container(
		"jacobalberty/firebird:3.0.4",
		expPort,
		map[string]string{
			"FIREBIRD_USER":     "gosql",
			"FIREBIRD_PASSWORD": "secret",
			"FIREBIRD_DATABASE": "gosql.fdb",
		},
		"firebirdsql",
		"gosql:secret@localhost:<port>//firebird/data/gosql.fdb",
		1,
	)

	defer server.Terminate(ctx)
	if err != nil {
		t.Error(err)
	}
	tm, theDB := InitFirebirdSQL(t, port.Port())
	tester := common.Tester{DbName: common.Firebird}
	tester.RunAll(tm, t)
	theDB.Close()
}

func InitFirebirdSQL(t *testing.T, port string) (ITransactionManager, *sql.DB) {

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

	return common.InitDB(
		t,
		"firebirdsql",
		fmt.Sprintf("gosql:secret@localhost:%s//firebird/data/gosql.fdb", port),
		translator,
		"tables_firebirdsql.sql",
	)
}
