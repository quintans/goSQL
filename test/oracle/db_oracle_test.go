package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/pkg/errors"
	. "github.com/quintans/goSQL/db"
	"github.com/quintans/goSQL/test/common"
	trx "github.com/quintans/goSQL/translators"
	"github.com/quintans/toolkit/log"
	"github.com/testcontainers/testcontainers-go"

	_ "gopkg.in/goracle.v2"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/test")

func StartContainer(port string) (func(), ITransactionManager, *sql.DB, error) {
	// check if there is an already running db instance
	closer := func() {}
	_, err := common.Connect("goracle", "gosql/gosql@localhost:1521/xe")
	if err != nil {
		expPort := "1521/tcp"
		ctx := context.Background()
		req := testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context: "./docker",
			},
			ExposedPorts: []string{expPort},
			WaitingFor: common.ForDb(
				"goracle",
				"gosql/gosql@localhost:<port>/xe",
				expPort,
			).WithStartupTimeout(time.Duration(1) * time.Minute),
		}
		db, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "To connect to Oracle, Instant Client is needed.")
		}

		closer = func() { db.Terminate(ctx) }

		pt, err := db.MappedPort(ctx, nat.Port(expPort))
		if err != nil {
			return nil, nil, nil, err
		}
		port = pt.Port()
	}

	tm, theDB, err := InitOracle(port)
	if err != nil {
		closer()
		return nil, nil, nil, err
	}
	return closer, tm, theDB, nil

}

func TestOracle(t *testing.T) {
	logger.Infof("******* Using Oracle *******\n")

	port := "1521"
	closer, tm, theDB, err := StartContainer(port)
	if err != nil {
		t.Fatal(err)
	}
	defer closer()

	tester := common.Tester{DbName: common.Oracle}
	tester.RunAll(tm, t)
	theDB.Close()
}

func InitOracle(port string) (ITransactionManager, *sql.DB, error) {
	common.RAW_SQL = "SELECT name FROM book WHERE name LIKE :1"

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

	return common.InitDB(
		"goracle",
		fmt.Sprintf("gosql/gosql@localhost:%s/xe", port),
		translator,
		"tables_oracle.sql",
	)
}
