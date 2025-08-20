package connector

import (
	"context"
	"github.com/Konsultn-Engineering/enorm/dialect"
)

type Provider interface {
	Connect(ctx context.Context, config Config) (Connection, error)
	Dialect() dialect.Dialect
	HealthCheck(ctx context.Context, conn Connection) error
}
