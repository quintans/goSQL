package test

import (
	. "github.com/quintans/goSQL/db"
	trx "github.com/quintans/goSQL/translators"

	_ "github.com/go-sql-driver/mysql"

	"database/sql"
	"fmt"
	"testing"
)

func InitMySQL5() (ITransactionManager, *sql.DB) {
	logger.Infof("******* Using MySQL5 *******\n")

	RAW_SQL = "SELECT `name` FROM `book` WHERE `name` LIKE ?"

	translator := trx.NewMySQL5Translator()
	/*
		registering custom function.
		A custom translator could be created instead.
	*/
	translator.RegisterTranslation(
		TOKEN_SECONDSDIFF,
		func(dmlType DmlType, token Tokener, tx Translator) string {
			m := token.GetMembers()
			return fmt.Sprintf(
				"TIME_TO_SEC(TIMEDIFF(%s, %s))",
				tx.Translate(dmlType, m[0]),
				tx.Translate(dmlType, m[1]),
			)
		},
	)

	return InitDB("mysql", "root:root@/gosql?parseTime=true", translator)
}

func TestMySQL5(t *testing.T) {
	tm, theDB := InitMySQL5()
	RunAll(tm, t)
	theDB.Close()
}
