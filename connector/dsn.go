// In connector/dsn.go
package connector

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// DSNBuilder provides a fluent interface for building database connection strings
type DSNBuilder struct {
	scheme   string
	username string
	password string
	host     string
	port     int
	database string
	params   map[string]string
}

// NewDSNBuilder creates a new DSN builder
func NewDSNBuilder(scheme string) *DSNBuilder {
	return &DSNBuilder{
		scheme: scheme,
		params: make(map[string]string),
	}
}

// Auth sets username and password
func (b *DSNBuilder) Auth(username, password string) *DSNBuilder {
	b.username = username
	b.password = password
	return b
}

// Host sets the host and port
func (b *DSNBuilder) Host(host string, port int) *DSNBuilder {
	b.host = host
	b.port = port
	return b
}

// Database sets the database name
func (b *DSNBuilder) Database(name string) *DSNBuilder {
	b.database = name
	return b
}

// Param adds a single parameter
func (b *DSNBuilder) Param(key, value string) *DSNBuilder {
	if value != "" {
		b.params[key] = value
	}
	return b
}

// Params adds multiple parameters
func (b *DSNBuilder) Params(params map[string]string) *DSNBuilder {
	for k, v := range params {
		if v != "" {
			b.params[k] = v
		}
	}
	return b
}

// Add defaults for common parameters
func (b *DSNBuilder) WithPostgresDefaults() *DSNBuilder {
	return b.Param("sslmode", "prefer").
		Param("connect_timeout", "10")
}

func (b *DSNBuilder) Validate() error {
	if b.host == "" {
		return fmt.Errorf("host is required")
	}
	if b.port <= 0 || b.port > 65535 {
		return fmt.Errorf("invalid port: %d", b.port)
	}
	return nil
}

// Build constructs the final DSN string
func (b *DSNBuilder) Build() string {
	var dsn strings.Builder

	// Scheme
	dsn.WriteString(b.scheme)
	dsn.WriteString("://")

	// Authentication
	if b.username != "" {
		dsn.WriteString(url.QueryEscape(b.username))
		if b.password != "" {
			dsn.WriteString(":")
			dsn.WriteString(url.QueryEscape(b.password))
		}
		dsn.WriteString("@")
	}

	// Host and port
	dsn.WriteString(b.host)
	if b.port > 0 {
		dsn.WriteString(":")
		dsn.WriteString(strconv.Itoa(b.port))
	}

	// Database
	if b.database != "" {
		dsn.WriteString("/")
		dsn.WriteString(url.PathEscape(b.database))
	}

	// Parameters
	if len(b.params) > 0 {
		dsn.WriteString("?")
		first := true
		for key, value := range b.params {
			if !first {
				dsn.WriteString("&")
			}
			dsn.WriteString(url.QueryEscape(key))
			dsn.WriteString("=")
			dsn.WriteString(url.QueryEscape(value))
			first = false
		}
	}

	return dsn.String()
}
