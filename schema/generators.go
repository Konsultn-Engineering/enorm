package schema

import (
	"crypto/rand"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
)

// IDGenerator defines the interface for ID generation
type IDGenerator interface {
	Generate() (any, error)
	Type() string
}

// UUIDGenerator generates UUID v4 values
type UUIDGenerator struct{}

func (g UUIDGenerator) Generate() (any, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate UUID: %w", err)
	}
	return id, nil
}

func (g UUIDGenerator) Type() string {
	return "uuid"
}

// ULIDGenerator generates ULID values
type ULIDGenerator struct {
	entropy *ulid.MonotonicEntropy
}

func NewULIDGenerator() *ULIDGenerator {
	entropy := ulid.Monotonic(rand.Reader, 0)
	return &ULIDGenerator{entropy: entropy}
}

func (g *ULIDGenerator) Generate() (any, error) {
	id, err := ulid.New(ulid.Timestamp(time.Now()), g.entropy)
	if err != nil {
		return nil, fmt.Errorf("failed to generate ULID: %w", err)
	}
	return id, nil
}

func (g *ULIDGenerator) Type() string {
	return "ulid"
}

// SnowflakeGenerator generates Twitter Snowflake-like IDs
type SnowflakeGenerator struct {
	machineID uint64
	sequence  uint64
	lastTime  uint64
	epoch     uint64 // Custom epoch (e.g., 2023-01-01)
}

func NewSnowflakeGenerator(machineID uint64) *SnowflakeGenerator {
	// Set epoch to 2023-01-01 00:00:00 UTC
	epoch := uint64(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
	return &SnowflakeGenerator{
		machineID: machineID & 0x3FF, // 10 bits
		epoch:     epoch,
	}
}

func (g *SnowflakeGenerator) Generate() (any, error) {
	now := uint64(time.Now().UnixMilli())

	if now < g.lastTime {
		return nil, fmt.Errorf("clock moved backwards")
	}

	if now == g.lastTime {
		g.sequence = (g.sequence + 1) & 0xFFF // 12 bits
		if g.sequence == 0 {
			// Wait for next millisecond
			for now <= g.lastTime {
				now = uint64(time.Now().UnixMilli())
			}
		}
	} else {
		g.sequence = 0
	}

	g.lastTime = now

	// Format: 41 bits timestamp | 10 bits machine | 12 bits sequence
	id := ((now - g.epoch) << 22) | (g.machineID << 12) | g.sequence

	return int64(id), nil
}

func (g *SnowflakeGenerator) Type() string {
	return "snowflake"
}

// NanoIDGenerator generates NanoID values
type NanoIDGenerator struct {
	size     int
	alphabet string
}

func NewNanoIDGenerator(size int, alphabet string) *NanoIDGenerator {
	if size <= 0 {
		size = 21 // Default NanoID size
	}
	if alphabet == "" {
		alphabet = "_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" // Default alphabet
	}
	return &NanoIDGenerator{size: size, alphabet: alphabet}
}

func (g *NanoIDGenerator) Generate() (any, error) {
	bytes := make([]byte, g.size)
	if _, err := rand.Read(bytes); err != nil {
		return nil, fmt.Errorf("failed to generate random bytes: %w", err)
	}

	id := make([]byte, g.size)
	for i := 0; i < g.size; i++ {
		id[i] = g.alphabet[bytes[i]%byte(len(g.alphabet))]
	}

	return string(id), nil
}

func (g *NanoIDGenerator) Type() string {
	return "nanoid"
}

// GeneratorRegistry manages ID generators
type GeneratorRegistry struct {
	generators map[string]IDGenerator
}

var defaultRegistry = NewGeneratorRegistry()

func NewGeneratorRegistry() *GeneratorRegistry {
	registry := &GeneratorRegistry{
		generators: make(map[string]IDGenerator),
	}

	// Register default generators
	registry.Register("uuid", UUIDGenerator{})
	registry.Register("ulid", NewULIDGenerator())
	registry.Register("snowflake", NewSnowflakeGenerator(1)) // Default machine ID
	registry.Register("nanoid", NewNanoIDGenerator(21, ""))

	return registry
}

func (r *GeneratorRegistry) Register(name string, generator IDGenerator) {
	r.generators[name] = generator
}

func (r *GeneratorRegistry) Get(name string) (IDGenerator, bool) {
	gen, ok := r.generators[name]
	return gen, ok
}

func (r *GeneratorRegistry) Generate(generatorType string) (any, error) {
	gen, ok := r.Get(generatorType)
	if !ok {
		return nil, fmt.Errorf("unknown generator type: %s", generatorType)
	}
	return gen.Generate()
}

// RegisterGenerator functions for convenience
func RegisterGenerator(name string, generator IDGenerator) {
	defaultRegistry.Register(name, generator)
}

func GenerateID(generatorType string) (any, error) {
	return defaultRegistry.Generate(generatorType)
}
