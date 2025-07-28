package connector

import (
	"context"
	"fmt"
	"sync"
)

type standardConnector struct {
	provider Provider
	config   Config
}

var globalManager = &Manager{
	providers: make(map[string]Provider),
}

type Manager struct {
	providers map[string]Provider
	mu        sync.RWMutex
}

func Register(name string, provider Provider) {
	globalManager.mu.Lock()
	defer globalManager.mu.Unlock()
	globalManager.providers[name] = provider
}

func New(name string, config Config) (Connector, error) {
	globalManager.mu.RLock()
	provider, ok := globalManager.providers[name]
	globalManager.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("provider %s not registered", name)
	}
	return &standardConnector{provider: provider, config: config}, nil
}

func (c *standardConnector) Connect(ctx context.Context) (Connection, error) {
	return c.provider.Connect(ctx, c.config)
}

func (c *standardConnector) ConnectWithRetry(ctx context.Context, opts RetryOptions) (Connection, error) {
	return c.retryConnect(ctx, opts)
}

func (c *standardConnector) Close() error {
	return nil
}
