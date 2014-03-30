package mysql

import (
	. "github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/test/common"
	trx "github.com/quintans/goSQL/translators"
	"github.com/quintans/toolkit/log"

	_ "github.com/go-sql-driver/mysql"

	"database/sql"
	"fmt"
	"testing"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/test")

func InitMySQL5() (ITransactionManager, *sql.DB) {
	logger.Infof("******* Using MySQL5 *******\n")

	common.RAW_SQL = "SELECT `name` FROM `book` WHERE `name` LIKE ?"

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

	return common.InitDB("mysql", "root:root@/gosql?parseTime=true", translator)
}

func TestMySQL5(t *testing.T) {
	tm, theDB := InitMySQL5()
	common.RunAll(tm, t)
	theDB.Close()
}
