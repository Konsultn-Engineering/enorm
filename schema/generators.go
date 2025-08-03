package schema

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
)

// IDGenerator defines the interface for generating unique identifiers.
// Implementations should be thread-safe for concurrent use in database operations.
type IDGenerator interface {
	// Generate creates a new unique identifier.
	// Returns the generated ID and any error encountered during generation.
	Generate() (any, error)

	// Type returns the string identifier for this generator type.
	// Used for registration and lookup in the generator registry.
	Type() string
}

// UUIDGenerator generates RFC 4122 compliant UUID v4 values.
// Thread-safe and suitable for distributed systems requiring globally unique identifiers.
type UUIDGenerator struct{}

// Generate creates a new random UUID v4.
// Returns uuid.UUID type that can be stored as string or binary in databases.
func (g UUIDGenerator) Generate() (any, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate UUID: %w", err)
	}
	return id, nil
}

// Type returns "uuid" as the generator identifier.
func (g UUIDGenerator) Type() string {
	return "uuid"
}

// ULIDGenerator generates Universally Unique Lexicographically Sortable Identifiers.
// ULIDs are 26 characters long, lexicographically sortable, and encode timestamp information.
// Thread-safe with monotonic entropy for same-millisecond generation ordering.
type ULIDGenerator struct {
	entropy *ulid.MonotonicEntropy
	mu      sync.Mutex // Protect entropy for thread safety
}

// NewULIDGenerator creates a new ULID generator with monotonic entropy.
// The monotonic entropy ensures proper ordering of IDs generated within the same millisecond.
func NewULIDGenerator() *ULIDGenerator {
	entropy := ulid.Monotonic(rand.Reader, 0)
	return &ULIDGenerator{entropy: entropy}
}

// Generate creates a new ULID with current timestamp and monotonic entropy.
// Returns ulid.ULID type that can be stored as string (26 chars) or binary (16 bytes).
func (g *ULIDGenerator) Generate() (any, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	id, err := ulid.New(ulid.Timestamp(time.Now()), g.entropy)
	if err != nil {
		return nil, fmt.Errorf("failed to generate ULID: %w", err)
	}
	return id, nil
}

// Type returns "ulid" as the generator identifier.
func (g *ULIDGenerator) Type() string {
	return "ulid"
}

// SnowflakeGenerator generates Twitter Snowflake-inspired 64-bit integers.
// Format: 41 bits timestamp | 10 bits machine ID | 12 bits sequence
// Provides roughly 4,096 IDs per millisecond per machine.
// Thread-safe and suitable for distributed systems with known machine IDs.
type SnowflakeGenerator struct {
	machineID uint64
	sequence  uint64
	lastTime  uint64
	epoch     uint64     // Custom epoch timestamp in milliseconds
	mu        sync.Mutex // Protect sequence and lastTime
}

// NewSnowflakeGenerator creates a new Snowflake generator with the specified machine ID.
// machineID should be unique per instance (0-1023, uses only 10 bits).
// Uses 2023-01-01 as epoch to maximize timestamp range.
func NewSnowflakeGenerator(machineID uint64) *SnowflakeGenerator {
	// Set epoch to 2023-01-01 00:00:00 UTC for extended timestamp range
	epoch := uint64(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
	return &SnowflakeGenerator{
		machineID: machineID & 0x3FF, // Mask to 10 bits (0-1023)
		epoch:     epoch,
	}
}

// Generate creates a new Snowflake ID as int64.
// Handles clock regression and sequence overflow with proper waiting.
// Returns int64 suitable for database integer primary keys.
func (g *SnowflakeGenerator) Generate() (any, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := uint64(time.Now().UnixMilli())

	// Handle clock regression (system clock moved backwards)
	if now < g.lastTime {
		return nil, fmt.Errorf("clock moved backwards: now=%d, last=%d", now, g.lastTime)
	}

	if now == g.lastTime {
		// Same millisecond: increment sequence
		g.sequence = (g.sequence + 1) & 0xFFF // Mask to 12 bits (0-4095)
		if g.sequence == 0 {
			// Sequence overflow: wait for next millisecond
			for now <= g.lastTime {
				now = uint64(time.Now().UnixMilli())
			}
		}
	} else {
		// New millisecond: reset sequence
		g.sequence = 0
	}

	g.lastTime = now

	// Compose final ID: timestamp(41) | machine(10) | sequence(12)
	id := ((now - g.epoch) << 22) | (g.machineID << 12) | g.sequence
	return int64(id), nil
}

// Type returns "snowflake" as the generator identifier.
func (g *SnowflakeGenerator) Type() string {
	return "snowflake"
}

// NanoIDGenerator generates URL-safe, compact unique identifiers.
// NanoIDs are shorter than UUIDs while maintaining similar collision resistance.
// Default size is 21 characters with ~137 years needed to have 1% probability of collision.
type NanoIDGenerator struct {
	size     int
	alphabet string
}

// NewNanoIDGenerator creates a new NanoID generator with specified parameters.
// size: length of generated IDs (default: 21 for good collision resistance)
// alphabet: characters to use (default: URL-safe base64 alphabet)
func NewNanoIDGenerator(size int, alphabet string) *NanoIDGenerator {
	if size <= 0 {
		size = 21 // Default size provides ~137 years to 1% collision probability
	}
	if alphabet == "" {
		// URL-safe alphabet (64 characters)
		alphabet = "_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	}
	return &NanoIDGenerator{size: size, alphabet: alphabet}
}

// Generate creates a new NanoID as a string.
// Uses cryptographically secure random number generation.
// Returns string suitable for URL parameters and database varchar fields.
func (g *NanoIDGenerator) Generate() (any, error) {
	bytes := make([]byte, g.size)
	if _, err := rand.Read(bytes); err != nil {
		return nil, fmt.Errorf("failed to generate random bytes: %w", err)
	}

	id := make([]byte, g.size)
	alphabetLen := byte(len(g.alphabet))
	for i := 0; i < g.size; i++ {
		id[i] = g.alphabet[bytes[i]%alphabetLen]
	}

	return string(id), nil
}

// Type returns "nanoid" as the generator identifier.
func (g *NanoIDGenerator) Type() string {
	return "nanoid"
}

// GeneratorRegistry manages a collection of ID generators with thread-safe access.
// Provides centralized registration and lookup of generators by type name.
// Pre-registered with common generator types for immediate use.
type GeneratorRegistry struct {
	generators map[string]IDGenerator
	mu         sync.RWMutex // Protect concurrent access
}

// Global default registry instance with pre-registered common generators.
// Available generators: "uuid", "ulid", "snowflake", "nanoid"
var defaultRegistry = NewGeneratorRegistry()

// NewGeneratorRegistry creates a new registry with default generators pre-registered.
// Default generators:
//   - "uuid": RFC 4122 UUID v4
//   - "ulid": Lexicographically sortable ULID
//   - "snowflake": Twitter Snowflake (machine ID: 1)
//   - "nanoid": 21-character NanoID
func NewGeneratorRegistry() *GeneratorRegistry {
	registry := &GeneratorRegistry{
		generators: make(map[string]IDGenerator),
	}

	// Register commonly used generators with sensible defaults
	registry.Register("uuid", UUIDGenerator{})
	registry.Register("ulid", NewULIDGenerator())
	registry.Register("snowflake", NewSnowflakeGenerator(1)) // Default machine ID
	registry.Register("nanoid", NewNanoIDGenerator(21, ""))  // Default size and alphabet

	return registry
}

// Register adds or replaces a generator in the registry.
// name: unique identifier for the generator
// generator: implementation of IDGenerator interface
// Thread-safe for concurrent registration.
func (r *GeneratorRegistry) Register(name string, generator IDGenerator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.generators[name] = generator
}

// Get retrieves a generator by name from the registry.
// Returns the generator and a boolean indicating if it was found.
// Thread-safe for concurrent access.
func (r *GeneratorRegistry) Get(name string) (IDGenerator, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	gen, ok := r.generators[name]
	return gen, ok
}

// Generate creates a new ID using the specified generator type.
// Convenience method that combines Get and Generate operations.
// Returns error if generator type is not registered.
func (r *GeneratorRegistry) Generate(generatorType string) (any, error) {
	gen, ok := r.Get(generatorType)
	if !ok {
		return nil, fmt.Errorf("unknown generator type: %s", generatorType)
	}
	return gen.Generate()
}

// RegisterGenerator adds a generator to the default global registry.
// Convenience function for simple generator registration.
// Thread-safe and suitable for package initialization.
func RegisterGenerator(name string, generator IDGenerator) {
	defaultRegistry.Register(name, generator)
}

// GenerateID creates a new ID using the specified generator from the default registry.
// Convenience function for one-off ID generation.
// Common usage: GenerateID("uuid"), GenerateID("ulid"), etc.
func GenerateID(generatorType string) (any, error) {
	return defaultRegistry.Generate(generatorType)
}
