package connector

type ConnectionStats struct {
	OpenConnections int
	InUse           int
	Idle            int
}
