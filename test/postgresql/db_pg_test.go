package postgresql

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/docker/go-connections/nat"
	. "github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/test/common"
	trx "github.com/quintans/goSQL/translators"
	"github.com/quintans/toolkit/log"

	_ "github.com/lib/pq"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/test")

func StartContainer() (func(), ITransactionManager, *sql.DB, nat.Port, error) {
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

	tester := common.Tester{DbName: common.Postgres}
	tester.RunAll(tm, t)
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

	tester := common.Tester{DbName: common.Postgres}
	tester.RunBench(tm, "postgres", fmt.Sprintf("dbname=postgres user=postgres password=secret port=%s sslmode=disable", port.Port()), b)
	theDB.Close()
}

func InitPostgreSQL(port string) (ITransactionManager, *sql.DB, error) {
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
		"postgres",
		fmt.Sprintf("dbname=postgres user=postgres password=secret port=%s sslmode=disable", port),
		translator,
		"tables_postgresql.sql",
	)
}
