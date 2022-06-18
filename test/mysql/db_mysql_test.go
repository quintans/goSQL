package mysql

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/docker/go-connections/nat"
	_ "github.com/go-sql-driver/mysql"
	. "github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/test/common"
	"github.com/quintans/goSQL/translators"
	"github.com/quintans/toolkit/log"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/test")

func StartContainer() (func(), ITransactionManager, *sql.DB, nat.Port, error) {
	expPort := "3306/tcp"
	ctx, server, port, err := common.Container(
		"mysql:5.7",
		expPort,
		map[string]string{"MYSQL_ROOT_PASSWORD": "secret"},
		"mysql",
		"root:secret@tcp(localhost:<port>)/mysql?parseTime=true",
		1,
	)

	if err != nil {
		return nil, nil, nil, "", err
	}

	closer := func() {
		server.Terminate(ctx)
	}

	tm, theDB, err := InitMySQL5(port.Port())
	if err != nil {
		closer()
		return nil, nil, nil, "", err
	}
	return closer, tm, theDB, port, nil
}

func TestMySQL5(t *testing.T) {
	logger.Infof("******* Using MySQL5 *******\n")

	closer, tm, theDB, _, err := StartContainer()
	if err != nil {
		t.Fatal(err)
	}
	defer closer()

	tester := common.Tester{DbName: common.MySQL, Tm: tm}
	tester.RunAll(t)
	theDB.Close()
}

func BenchmarkLoadValues(b *testing.B) {
	logger.Infof("******* Benchmarking Using MySQL *******\n")
	log.Register("/", log.ERROR)
	closer, tm, theDB, port, err := StartContainer()
	if err != nil {
		b.Fatal(err)
	}
	defer closer()

	tester := common.Tester{DbName: common.MySQL, Tm: tm}
	tester.RunBench(
		"mysql",
		fmt.Sprintf("root:secret@tcp(localhost:%s)/mysql?parseTime=true", port.Port()),
		"EMPLOYEE",
		b,
	)
	theDB.Close()
}

func InitMySQL5(port string) (ITransactionManager, *sql.DB, error) {
	common.RAW_SQL = "SELECT NAME FROM BOOK WHERE NAME LIKE ?"

	translator := translators.NewMySQL5Translator()
	/*
		registering custom function.
		A custom translator could be created instead.
	*/
	translator.RegisterTranslation(
		common.TOKEN_SECONDSDIFF,
		func(dmlType DmlType, token Tokener, tx Translator) (string, error) {
			m := token.GetMembers()
			args, err := translators.Translate(tx.Translate, dmlType, m...)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf(
				"TIME_TO_SEC(TIMEDIFF(%s, %s))",
				args[0],
				args[1],
			), nil
		},
	)

	return common.InitDB(
		"mysql",
		fmt.Sprintf("root:secret@tcp(localhost:%s)/mysql?parseTime=true", port),
		translator,
		"tables_mysql.sql",
	)
}
