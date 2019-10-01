package mysql

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/test/common"
	trx "github.com/quintans/goSQL/translators"
	"github.com/quintans/toolkit/log"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/test")

func TestMySQL5(t *testing.T) {
	logger.Infof("******* Using MySQL5 *******\n")

	expPort := "3306/tcp"
	ctx, server, port, err := common.Container(
		"mysql:5.7",
		expPort,
		map[string]string{"MYSQL_ROOT_PASSWORD": "secret"},
		"mysql",
		"root:secret@tcp(localhost:<port>)/mysql?parseTime=true",
		1,
	)

	defer server.Terminate(ctx)
	if err != nil {
		t.Error(err)
	}

	tm, theDB := InitMySQL5(t, port.Port())
	tester := common.Tester{DbName: common.MySQL}
	tester.RunAll(tm, t)
	theDB.Close()
}

func InitMySQL5(t *testing.T, port string) (ITransactionManager, *sql.DB) {
	common.RAW_SQL = "SELECT `NAME` FROM `BOOK` WHERE `NAME` LIKE ?"

	translator := trx.NewMySQL5Translator()
	/*
		registering custom function.
		A custom translator could be created instead.
	*/
	translator.RegisterTranslation(
		common.TOKEN_SECONDSDIFF,
		func(dmlType DmlType, token Tokener, tx Translator) string {
			m := token.GetMembers()
			return fmt.Sprintf(
				"TIME_TO_SEC(TIMEDIFF(%s, %s))",
				tx.Translate(dmlType, m[0]),
				tx.Translate(dmlType, m[1]),
			)
		},
	)

	return common.InitDB(
		t,
		"mysql",
		fmt.Sprintf("root:secret@tcp(localhost:%s)/mysql?parseTime=true", port),
		translator,
		"tables_mysql.sql",
	)
}
