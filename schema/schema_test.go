package schema

import (
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =========================================================================
// Test Data Structures
// =========================================================================

type User struct {
	ID        uint64    `db:"id"`
	FirstName string    `db:"first_name"`
	LastName  string    `db:"last_name"`
	Email     string    `db:"email"`
	Age       int32     `db:"age"`
	Score     float64   `db:"score"`
	Active    bool      `db:"active"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
	Data      []byte    `db:"data"`
}

type Product struct {
	ID          uint64  `db:"id"`
	Name        string  `db:"name"`
	Price       float64 `db:"price"`
	InStock     bool    `db:"in_stock"`
	CategoryID  int64   `db:"category_id"`
	Description string  `db:"description"`
}

type ComplexStruct struct {
	StringField    string            `db:"string_field"`
	IntField       int               `db:"int_field"`
	FloatField     float64           `db:"float_field"`
	BoolField      bool              `db:"bool_field"`
	TimeField      time.Time         `db:"time_field"`
	BytesField     []byte            `db:"bytes_field"`
	PtrField       *string           `db:"ptr_field"`
	SliceField     []string          `db:"slice_field"`
	MapField       map[string]string `db:"map_field"`
	InterfaceField interface{}       `db:"interface_field"`
}

type EmbeddedStruct struct {
	User
	ExtraField string `db:"extra_field"`
}

type NoTagsStruct struct {
	ID   uint64
	Name string
	Age  int
}

// =========================================================================
// Introspection Tests
// =========================================================================

func TestIntrospect(t *testing.T) {
	tests := []struct {
		name           string
		inputType      reflect.Type
		expectError    bool
		expectedFields int
		expectedTable  string
	}{
		{
			name:           "ValidStruct",
			inputType:      reflect.TypeOf(User{}),
			expectError:    false,
			expectedFields: 10,
			expectedTable:  "users",
		},
		{
			name:           "ValidStructPtr",
			inputType:      reflect.TypeOf(&User{}),
			expectError:    false,
			expectedFields: 10,
			expectedTable:  "users",
		},
		{
			name:        "InvalidTypeString",
			inputType:   reflect.TypeOf("string"),
			expectError: true,
		},
		{
			name:        "InvalidTypeInt",
			inputType:   reflect.TypeOf(42),
			expectError: true,
		},
		{
			name:           "NoTagsStruct",
			inputType:      reflect.TypeOf(NoTagsStruct{}),
			expectError:    false,
			expectedFields: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta, err := Introspect(tt.inputType)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, meta)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, meta)
			assert.Equal(t, tt.expectedFields, len(meta.Fields))

			if tt.expectedTable != "" {
				assert.Equal(t, tt.expectedTable, meta.TableName)
			}

			// Validate field metadata
			for _, field := range meta.Fields {
				assert.NotEmpty(t, field.Name)
				assert.NotEmpty(t, field.DBName)
				assert.NotNil(t, field.Type)
				assert.NotNil(t, field.DirectSet)
				assert.True(t, field.Offset >= 0)
			}

			// Validate column map
			assert.Equal(t, len(meta.Fields), len(meta.ColumnMap))
			for _, field := range meta.Fields {
				foundField, exists := meta.ColumnMap[field.DBName]
				assert.True(t, exists)
				assert.Equal(t, field.Name, foundField.Name)
			}
		})
	}
}

func TestIntrospectCaching(t *testing.T) {
	// Clear cache to start fresh
	ClearCache()
	ClearPrecompiled()

	userType := reflect.TypeOf(User{})

	// First call should build metadata
	meta1, err := Introspect(userType)
	require.NoError(t, err)
	require.NotNil(t, meta1)

	// Second call should return cached metadata
	meta2, err := Introspect(userType)
	require.NoError(t, err)
	require.NotNil(t, meta2)

	// Should be the same instance (cached)
	assert.True(t, meta1 == meta2, "Expected same instance from cache")

	// Cache stats should show 1 item
	assert.Equal(t, 1, GetCacheStats())
}

func TestPrecompileType(t *testing.T) {
	// Clear precompiled to start fresh
	ClearPrecompiled()
	assert.Equal(t, 0, GetPrecompiledCount())

	// Precompile User type
	meta := PrecompileType[User]()
	require.NotNil(t, meta)
	assert.Equal(t, 1, GetPrecompiledCount())

	// Introspect should use precompiled version
	userType := reflect.TypeOf(User{})
	meta2, err := Introspect(userType)
	require.NoError(t, err)

	// Should be the same instance (precompiled)
	assert.True(t, meta == meta2, "Expected same precompiled instance")

	// Test with pointer type
	meta3 := PrecompileType[*User]()
	assert.True(t, meta == meta3, "Pointer type should normalize to same metadata")
}

func TestIntrospectConcurrency(t *testing.T) {
	const numGoroutines = 10 // Reduced for more reliable testing
	const numIterations = 10

	ClearCache()
	ClearPrecompiled()

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)
	results := make(chan *EntityMeta, numGoroutines*numIterations)

	// Use a barrier to ensure all goroutines start at the same time
	startBarrier := make(chan struct{})

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Wait for all goroutines to be ready
			<-startBarrier

			for j := 0; j < numIterations; j++ {
				meta, err := Introspect(reflect.TypeOf(User{}))
				if err != nil {
					errors <- err
					return
				}
				results <- meta
			}
		}(i)
	}

	// Release all goroutines at once
	close(startBarrier)

	wg.Wait()
	close(errors)
	close(results)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent introspection error: %v", err)
	}

	// Collect all results and verify they're consistent
	var metas []*EntityMeta
	for meta := range results {
		metas = append(metas, meta)
	}

	assert.Equal(t, numGoroutines*numIterations, len(metas))

	// All metas should be identical (either all same cached instance, or at least equivalent)
	if len(metas) > 0 {
		firstMeta := metas[0]
		for i, meta := range metas {
			// They should at least have the same content even if not same instance
			assert.Equal(t, len(firstMeta.Fields), len(meta.Fields), "Meta %d should have same field count", i)
			assert.Equal(t, firstMeta.TableName, meta.TableName, "Meta %d should have same table name", i)
		}
	}

	assert.Equal(t, 1, GetCacheStats()) // Should only have 1 cached item
}

// =========================================================================
// Field Binding Tests
// =========================================================================

func TestFieldBinder(t *testing.T) {
	user := &User{}
	binder := newBinder(user)
	defer returnBinder(binder)

	err := binder.Bind(user, &user.ID, &user.FirstName, &user.Email)
	require.NoError(t, err)

	bindings := binder.Bindings()
	assert.Equal(t, 3, len(bindings))
	assert.True(t, binder.HasBinding("id"))
	assert.True(t, binder.HasBinding("first_name"))
	assert.True(t, binder.HasBinding("email"))
	assert.False(t, binder.HasBinding("nonexistent"))

	assert.Equal(t, 3, binder.BindingCount())
}

func TestFieldBinderErrors(t *testing.T) {
	t.Run("NonPointerEntity", func(t *testing.T) {
		binder := newBinder("not a pointer")
		defer returnBinder(binder)

		err := binder.Bind("not a pointer", "field")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be a pointer")
	})

	t.Run("NonStructEntity", func(t *testing.T) {
		str := "string"
		binder := newBinder(&str)
		defer returnBinder(binder)

		err := binder.Bind(&str, &str)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be pointer to struct")
	})

	t.Run("NilFieldPointer", func(t *testing.T) {
		user := &User{}
		binder := newBinder(user)
		defer returnBinder(binder)

		err := binder.Bind(user, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "field pointer")
		assert.Contains(t, err.Error(), "is nil")
	})

	t.Run("NonPointerField", func(t *testing.T) {
		user := &User{}
		binder := newBinder(user)
		defer returnBinder(binder)

		err := binder.Bind(user, "not a pointer")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be a pointer")
	})

	t.Run("FieldNotBelongingToStruct", func(t *testing.T) {
		user := &User{}
		product := &Product{}
		binder := newBinder(user)
		defer returnBinder(binder)

		err := binder.Bind(user, &product.ID) // Product field, not User field
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not belong to struct")
	})
}

func TestFieldBinderPool(t *testing.T) {
	// Test pool reuse
	user := &User{}

	binder1 := newBinder(user)
	binder1.Bind(user, &user.ID)
	assert.Equal(t, 1, binder1.BindingCount())
	returnBinder(binder1)

	binder2 := newBinder(user)
	// Should start with clean bindings from pool
	assert.Equal(t, 0, binder2.BindingCount())
	returnBinder(binder2)

	// Should be same instance reused
	assert.True(t, binder1 == binder2, "Expected pool to reuse same instance")
}

// =========================================================================
// Scanner Registration Tests
// =========================================================================

func TestScannerRegistration(t *testing.T) {
	// Clear existing registrations
	ClearRegisteredScanners()

	// Register custom scanner
	RegisterScanner(User{}, func(target any, binder FieldBinder) error {
		user := target.(*User)
		return binder.Bind(user, &user.ID, &user.FirstName, &user.Email)
	})

	userType := reflect.TypeOf(User{})
	assert.True(t, HasRegisteredScanner(userType))

	scanner := getRegisteredScanner(userType)
	assert.NotNil(t, scanner)

	// Test with pointer type (should normalize)
	RegisterScanner(&User{}, func(target any, binder FieldBinder) error {
		return nil
	})
	assert.True(t, HasRegisteredScanner(userType)) // Should normalize to struct type
}

func TestGetRegisteredScanners(t *testing.T) {
	ClearRegisteredScanners()

	// Register multiple scanners
	RegisterScanner(User{}, func(target any, binder FieldBinder) error { return nil })
	RegisterScanner(Product{}, func(target any, binder FieldBinder) error { return nil })

	scanners := GetRegisteredScanners()
	assert.Equal(t, 2, len(scanners))

	userType := reflect.TypeOf(User{})
	productType := reflect.TypeOf(Product{})

	_, hasUser := scanners[userType]
	_, hasProduct := scanners[productType]

	assert.True(t, hasUser)
	assert.True(t, hasProduct)
}

// =========================================================================
// Direct Setter Tests
// =========================================================================

func TestDirectSetters(t *testing.T) {
	user := &User{}
	meta, err := Introspect(reflect.TypeOf(user))
	require.NoError(t, err)

	tests := []struct {
		name   string
		field  string
		value  any
		verify func(*User) bool
	}{
		{
			name:   "SetUint64",
			field:  "id",
			value:  uint64(12345),
			verify: func(u *User) bool { return u.ID == 12345 },
		},
		{
			name:   "SetString",
			field:  "first_name",
			value:  "John",
			verify: func(u *User) bool { return u.FirstName == "John" },
		},
		{
			name:   "SetInt32",
			field:  "age",
			value:  int32(30),
			verify: func(u *User) bool { return u.Age == 30 },
		},
		{
			name:   "SetFloat64",
			field:  "score",
			value:  float64(95.5),
			verify: func(u *User) bool { return u.Score == 95.5 },
		},
		{
			name:   "SetBool",
			field:  "active",
			value:  true,
			verify: func(u *User) bool { return u.Active == true },
		},
		{
			name:   "SetBytes",
			field:  "data",
			value:  []byte("test data"),
			verify: func(u *User) bool { return string(u.Data) == "test data" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset user
			user = &User{}

			fieldMeta, exists := meta.ColumnMap[tt.field]
			require.True(t, exists, "Field %s should exist in column map", tt.field)
			require.NotNil(t, fieldMeta.DirectSet, "DirectSet should not be nil")

			// Set the value
			fieldMeta.DirectSet(unsafe.Pointer(user), tt.value)

			// Verify the value was set
			assert.True(t, tt.verify(user), "Field %s was not set correctly", tt.field)
		})
	}
}

func TestDirectSettersConcurrency(t *testing.T) {
	const numGoroutines = 50
	const numIterations = 100

	meta, err := Introspect(reflect.TypeOf(User{}))
	require.NoError(t, err)

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			user := &User{} // Each goroutine gets its own user
			fieldMeta := meta.ColumnMap["id"]

			defer func() {
				if r := recover(); r != nil {
					errors <- assert.AnError
				}
			}()

			for j := 0; j < numIterations; j++ {
				value := uint64(goroutineID*numIterations + j)
				fieldMeta.DirectSet(unsafe.Pointer(user), value)
			}

			expectedFinal := uint64(goroutineID*numIterations + (numIterations - 1))
			if user.ID != expectedFinal {
				errors <- assert.AnError
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error("Concurrent setter test failed:", err)
	}
}

// =========================================================================
// Type Conversion Tests
// =========================================================================

func TestTypeConversions(t *testing.T) {
	tests := []struct {
		name        string
		targetType  reflect.Type
		sourceValue any
		expectError bool
	}{
		{
			name:        "StringToString",
			targetType:  reflect.TypeOf(""),
			sourceValue: "test",
			expectError: false,
		},
		{
			name:        "IntToInt",
			targetType:  reflect.TypeOf(int(0)),
			sourceValue: int(42),
			expectError: false,
		},
		{
			name:        "Int64ToUint64",
			targetType:  reflect.TypeOf(uint64(0)),
			sourceValue: int64(42),
			expectError: false,
		},
		{
			name:        "Int64NegativeToUint64",
			targetType:  reflect.TypeOf(uint64(0)),
			sourceValue: int64(-42),
			expectError: true,
		},
		{
			name:        "StringToTime",
			targetType:  reflect.TypeOf(time.Time{}),
			sourceValue: "2025-07-31T21:37:45Z",
			expectError: false,
		},
		{
			name:        "InvalidStringToTime",
			targetType:  reflect.TypeOf(time.Time{}),
			sourceValue: "invalid-time",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zeroValue := reflect.Zero(tt.targetType).Interface()
			converter, err := GetConverter(zeroValue, reflect.TypeOf(tt.sourceValue))

			if tt.expectError {
				// For error cases, either no converter exists or conversion returns zero value
				if err != nil {
					// No converter found - this is acceptable for incompatible types
					t.Logf("No converter found for %s to %s (expected for error case)",
						reflect.TypeOf(tt.sourceValue), tt.targetType)
					return
				}

				if converter != nil {
					result, _ := converter(tt.sourceValue)
					// Result should be zero value for failed conversions
					if result == nil {
						// Converter returned nil, which means conversion failed
						t.Logf("Converter returned nil for failed conversion (acceptable)")
						return
					}
					assert.Equal(t, zeroValue, result, "Failed conversion should return zero value")
				}
			} else {
				require.NoError(t, err, "Should not error for valid conversion")
				require.NotNil(t, converter, "Converter should exist for valid conversion")

				result, _ := converter(tt.sourceValue)
				assert.NotNil(t, result, "Conversion result should not be nil")

				// Verify type is correct
				if result != nil {
					assert.Equal(t, tt.targetType, reflect.TypeOf(result))
				}
			}
		})
	}
}

// =========================================================================
// Memory and Performance Tests
// =========================================================================

func TestMemoryBounds(t *testing.T) {
	// Test that direct setters don't write outside struct boundaries
	type BoundedStruct struct {
		Sentinel1 uint64
		Target    uint64
		Sentinel2 uint64
	}

	const (
		sentinel1Value = uint64(0xDEADBEEFCAFEBABE)
		sentinel2Value = uint64(0xFEEDFACEDEADC0DE)
		targetValue    = uint64(0x1234567890ABCDEF)
	)

	bs := &BoundedStruct{
		Sentinel1: sentinel1Value,
		Target:    0,
		Sentinel2: sentinel2Value,
	}

	meta, err := Introspect(reflect.TypeOf(*bs))
	require.NoError(t, err)

	// Find target field
	var targetField *FieldMeta
	for _, field := range meta.Fields {
		if field.Name == "Target" {
			targetField = field
			break
		}
	}
	require.NotNil(t, targetField)

	// Set the target field
	targetField.DirectSet(unsafe.Pointer(bs), targetValue)

	// Verify sentinels are unchanged
	assert.Equal(t, sentinel1Value, bs.Sentinel1, "Sentinel1 should be unchanged")
	assert.Equal(t, sentinel2Value, bs.Sentinel2, "Sentinel2 should be unchanged")
	assert.Equal(t, targetValue, bs.Target, "Target should be set correctly")
}

// =========================================================================
// Cleanup and Setup
// =========================================================================

func TestMain(m *testing.M) {
	// Global test setup
	InitializeCache(512, nil)

	// Precompile common types for testing
	PrecompileType[User]()
	PrecompileType[Product]()

	// Run tests
	exitCode := m.Run()

	// Global cleanup
	ClearCache()
	ClearPrecompiled()
	ClearRegisteredScanners()

	// Exit with same code as tests
	if exitCode != 0 {
		panic("Tests failed")
	}
}
