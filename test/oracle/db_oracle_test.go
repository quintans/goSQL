package oracle

import (
	. "github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/test/common"
	trx "github.com/quintans/goSQL/translators"
	"github.com/quintans/toolkit/log"

	//_ "github.com/mattn/go-oci8"
	_ "github.com/tgulacsi/goracle/godrv"
	"github.com/tgulacsi/goracle/oracle"

	"database/sql"
	"fmt"
	"testing"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/test")

func InitOracle() (ITransactionManager, *sql.DB) {
	logger.Infof("******* Using Oracle *******\n")

	common.RAW_SQL = "SELECT name FROM book WHERE name LIKE ?"

	translator := trx.NewOracleTranslator()
	translator.RegisterTranslation(
		common.TOKEN_SECONDSDIFF,
		func(dmlType DmlType, token Tokener, tx Translator) string {
			m := token.GetMembers()
			return fmt.Sprintf(
				"(SYSDATE - ( %s - %s) - SYSDATE)*86400",
				tx.Translate(dmlType, m[1]),
				tx.Translate(dmlType, m[0]),
			)
		},
	)

	//var sid = oracle.MakeDSN("oracleserver", 1521, "", "XE")
	var sid = oracle.MakeDSN("192.168.56.101", 1521, "", "XE")
	return common.InitDB("goracle", "gosql/gosql@"+sid, translator)
}

func TestOracle(t *testing.T) {
	tm, theDB := InitOracle()
	common.RunAll(tm, t)
	theDB.Close()
}
