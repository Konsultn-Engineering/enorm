package connector

import (
	"context"
	"database/sql"
	"github.com/Konsultn-Engineering/enorm/dialect"
	"time"
)

type Connection interface {
	DB() *sql.DB
	Dialect() dialect.Dialect
	Health(ctx context.Context) error
	Stats() ConnectionStats
	Close() error
}

type Config struct {
	Host           string            `json:"host" yaml:"host"`
	Port           int               `json:"port" yaml:"port"`
	Database       string            `json:"database" yaml:"database"`
	Username       string            `json:"username" yaml:"username"`
	Password       string            `json:"password" yaml:"password"`
	SSLMode        string            `json:"ssl_mode" yaml:"ssl_mode"`
	Params         map[string]string `json:"params" yaml:"params"`
	Pool           PoolConfig        `json:"pool" yaml:"pool"`
	ConnectTimeout time.Duration     `json:"connect_timeout" yaml:"connect_timeout"`
	QueryTimeout   time.Duration     `json:"query_timeout" yaml:"query_timeout"`
}

type PoolConfig struct {
	MaxOpen         int           `json:"max_open" yaml:"max_open"`
	MaxIdle         int           `json:"max_idle" yaml:"max_idle"`
	MaxLifetime     time.Duration `json:"max_lifetime" yaml:"max_lifetime"`
	MaxIdleTime     time.Duration `json:"max_idle_time" yaml:"max_idle_time"`
	HealthCheckFreq time.Duration `json:"health_check_freq" yaml:"health_check_freq"`
}

type Connector interface {
	Connect(ctx context.Context) (Connection, error)
	ConnectWithRetry(ctx context.Context, opts RetryOptions) (Connection, error)
	Close() error
}
