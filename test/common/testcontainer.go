package common

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Implement interface
var _ wait.Strategy = (*DbStrategy)(nil)

// DbStrategy will wait until a given log entry shows up in the docker logs
type DbStrategy struct {
	// all Strategies should have a startupTimeout to avoid waiting infinitely
	startupTimeout time.Duration

	// additional properties
	driverName     string
	dataSourceName string
	port           nat.Port
	PollInterval   time.Duration
}

// ForDb constructs a HTTP strategy waiting on port 80 and status code 200
func ForDb(driverName string, dataSourceName string, port string) *DbStrategy {
	return &DbStrategy{
		startupTimeout: 60 * time.Second,
		driverName:     driverName,
		dataSourceName: dataSourceName,
		port:           nat.Port(port),
		PollInterval:   500 * time.Millisecond,
	}

}

// fluent builders for each property
// since go has neither covariance nor generics, the return type must be the type of the concrete implementation
// this is true for all properties, even the "shared" ones like startupTimeout

// WithStartupTimeout can be used to change the default startup timeout
func (ws *DbStrategy) WithStartupTimeout(startupTimeout time.Duration) *DbStrategy {
	ws.startupTimeout = startupTimeout
	return ws
}

// WithPollInterval can be used to override the default polling interval of 100 milliseconds
func (ws *DbStrategy) WithPollInterval(pollInterval time.Duration) *DbStrategy {
	ws.PollInterval = pollInterval
	return ws
}

// WaitUntilReady implements Strategy.WaitUntilReady
func (ws *DbStrategy) WaitUntilReady(ctx context.Context, target wait.StrategyTarget) (err error) {
	// limit context to startupTimeout
	ctx, cancelContext := context.WithTimeout(ctx, ws.startupTimeout)
	defer cancelContext()

	if strings.Index(ws.dataSourceName, "<port>") < 0 {
		return errors.New("Missing placeholder <port> in " + ws.dataSourceName)
	}

	var ds string
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			var err error
			if ds == "" {
				var port nat.Port
				port, err = target.MappedPort(ctx, ws.port)
				if err == nil {
					ds = strings.ReplaceAll(ws.dataSourceName, "<port>", port.Port())
				}
			}

			if err == nil {
				_, err = Connect(ws.driverName, ds)
			}
			if err != nil {
				time.Sleep(ws.PollInterval)
				continue
			}

			return nil
		}
	}
}

func Container(
	image string,
	exPort string,
	env map[string]string,
	driverName string,
	dataSourceName string,
	timeout int,
) (context.Context, testcontainers.Container, nat.Port, error) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{exPort},
		Env:          env,
		WaitingFor: ForDb(
			driverName,
			dataSourceName,
			exPort,
		).WithStartupTimeout(time.Duration(timeout) * time.Minute),
	}
	server, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	var port nat.Port
	if err == nil {
		port, err = server.MappedPort(ctx, nat.Port(exPort))
	}
	return ctx, server, port, err
}
