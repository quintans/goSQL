package postgresql

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/docker/go-connections/nat"
	. "github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/test/common"
	"github.com/quintans/goSQL/translators"
	"github.com/quintans/toolkit/log"

	_ "github.com/lib/pq"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/test")

func StartContainer() (func(), *TransactionManager, *sql.DB, nat.Port, error) {
	expPort := "5432/tcp"
	ctx, server, port, err := common.Container(
		"postgres:9.6.8",
		expPort,
		map[string]string{"POSTGRES_PASSWORD": "secret"},
		"postgres",
		"dbname=postgres user=postgres password=secret port=<port> sslmode=disable",
		1,
	)
	if err != nil {
		return nil, nil, nil, "", err
	}

	closer := func() {
		server.Terminate(ctx)
	}

	tm, theDB, err := InitPostgreSQL(port.Port())
	if err != nil {
		closer()
		return nil, nil, nil, "", err
	}
	return closer, tm, theDB, port, nil
}

func TestPostgreSQL(t *testing.T) {
	logger.Infof("******* Using PostgreSQL *******\n")

	closer, tm, theDB, _, err := StartContainer()
	if err != nil {
		t.Fatal(err)
	}
	defer closer()

	tester := common.Tester{DbName: common.Postgres, Tm: tm}
	tester.RunAll(t)
	theDB.Close()
}

func BenchmarkLoadValues(b *testing.B) {
	logger.Infof("******* Benchmarking Using PostgreSQL *******\n")
	log.Register("/", log.ERROR)
	closer, tm, theDB, port, err := StartContainer()
	if err != nil {
		b.Fatal(err)
	}
	defer closer()

	tester := common.Tester{DbName: common.Postgres, Tm: tm}
	tester.RunBench(
		"postgres",
		fmt.Sprintf("dbname=postgres user=postgres password=secret port=%s sslmode=disable", port.Port()),
		"employee",
		b,
	)
	theDB.Close()
}

func InitPostgreSQL(port string) (*TransactionManager, *sql.DB, error) {
	common.RAW_SQL = "SELECT name FROM book WHERE name LIKE $1"

	translator := translators.NewPostgreSQLTranslator()
	translator.RegisterTranslation(
		common.TOKEN_SECONDSDIFF,
		func(dmlType DmlType, token Tokener, tx Translator) (string, error) {
			m := token.GetMembers()
			args, err := translators.Translate(tx.Translate, dmlType, m...)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf(
				"EXTRACT(EPOCH FROM (%s - %s))",
				args[0],
				args[1],
			), nil
		},
	)

	return common.InitDB(
		"postgres",
		fmt.Sprintf("dbname=postgres user=postgres password=secret port=%s sslmode=disable", port),
		translator,
		"tables_postgresql.sql",
	)
}
