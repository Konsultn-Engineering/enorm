package connector

// ConnectionStats represents database connection pool statistics.
type ConnectionStats struct {
	OpenConnections int
	InUse           int
	Idle            int
}
