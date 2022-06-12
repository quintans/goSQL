package firebird

import (
	. "github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/test/common"
	"github.com/quintans/goSQL/translators"
	"github.com/quintans/toolkit/log"

	_ "github.com/nakagami/firebirdsql" // float64 was fixed acording to issue #5

	"database/sql"
	"fmt"
	"testing"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/test")

func StartContainer() (func(), ITransactionManager, *sql.DB, error) {
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

	if err != nil {
		return nil, nil, nil, err
	}
	closer := func() {
		server.Terminate(ctx)
	}

	tm, theDB, err := InitFirebirdSQL(port.Port())
	if err != nil {
		closer()
		return nil, nil, nil, err
	}
	return closer, tm, theDB, nil
}

func TestFirebirdSQL(t *testing.T) {
	logger.Infof("******* Using FirebirdSQL *******\n")

	closer, tm, theDB, err := StartContainer()
	if err != nil {
		t.Fatal(err)
	}
	defer closer()

	tester := common.Tester{DbName: common.Firebird, Tm: tm}
	tester.RunAll(t)
	theDB.Close()
}

func InitFirebirdSQL(port string) (ITransactionManager, *sql.DB, error) {

	common.RAW_SQL = "SELECT name FROM book WHERE name LIKE ?"

	translator := translators.NewFirebirdSQLTranslator()
	translator.RegisterTranslation(
		common.TOKEN_SECONDSDIFF,
		func(dmlType DmlType, token Tokener, tx Translator) (string, error) {
			m := token.GetMembers()
			args, err := translators.Translate(tx.Translate, dmlType, m...)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf(
				"DATEDIFF(SECOND, %s, %s)",
				args[1],
				args[0],
			), nil
		},
	)

	return common.InitDB(
		"firebirdsql",
		fmt.Sprintf("gosql:secret@localhost:%s//firebird/data/gosql.fdb", port),
		translator,
		"tables_firebirdsql.sql",
	)
}
