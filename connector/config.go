package connector

import (
	"fmt"
	"time"
)

// Config represents database connection configuration.
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
	Retry          *RetryConfig      `json:"retry,omitempty" yaml:"retry,omitempty"`
}

// PoolConfig defines connection pool settings.
type PoolConfig struct {
	MaxOpen         int           `json:"max_open" yaml:"max_open"`
	MaxIdle         int           `json:"max_idle" yaml:"max_idle"`
	MaxLifetime     time.Duration `json:"max_lifetime" yaml:"max_lifetime"`
	MaxIdleTime     time.Duration `json:"max_idle_time" yaml:"max_idle_time"`
	HealthCheckFreq time.Duration `json:"health_check_freq" yaml:"health_check_freq"`
}

// RetryConfig defines connection retry behavior.
type RetryConfig struct {
	MaxRetries int           `json:"max_retries" yaml:"max_retries"`
	BaseDelay  time.Duration `json:"base_delay" yaml:"base_delay"`
	MaxDelay   time.Duration `json:"max_delay" yaml:"max_delay"`
	Backoff    float64       `json:"backoff" yaml:"backoff"`
}

// ClusterConfig defines primary-replica database cluster configuration.
type ClusterConfig struct {
	Primary       Config        `json:"primary" yaml:"primary"`
	Replicas      []Config      `json:"replicas" yaml:"replicas"`
	ReadStrategy  string        `json:"read_strategy" yaml:"read_strategy"`
	WriteStrategy string        `json:"write_strategy" yaml:"write_strategy"`
	FailoverDelay time.Duration `json:"failover_delay" yaml:"failover_delay"`
}

// ValidateCluster validates cluster configuration.
func (cc *ClusterConfig) ValidateCluster() error {
	if cc.Primary.Host == "" {
		return fmt.Errorf("primary host is required")
	}

	validStrategies := map[string]bool{
		"round_robin": true,
		"random":      true,
		"primary":     true,
		"closest":     true,
	}

	if cc.ReadStrategy != "" && !validStrategies[cc.ReadStrategy] {
		return fmt.Errorf("invalid read strategy: %s", cc.ReadStrategy)
	}

	if cc.WriteStrategy != "" && cc.WriteStrategy != "primary" {
		return fmt.Errorf("invalid write strategy: %s (only 'primary' supported)", cc.WriteStrategy)
	}

	return nil
}
