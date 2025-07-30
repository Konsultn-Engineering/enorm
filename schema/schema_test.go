package schema

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"
)

// Test structures for validation
type TestStruct struct {
	ID        uint64
	Name      string
	Age       int32
	Score     float64
	Active    bool
	CreatedAt time.Time
	Data      []byte
	Padding   [8]byte // For alignment testing
}

type SmallStruct struct {
	A uint8
	B uint16
	C uint32
}

type LargeStruct struct {
	Field1 [1000]byte
	Field2 string
	Field3 uint64
	Field4 [2000]byte
}

// TestBasicFieldSetting tests basic unsafe pointer field setting
func TestBasicFieldSetting(t *testing.T) {
	tests := []struct {
		name        string
		fieldName   string
		value       any
		expected    any
		shouldPanic bool
	}{
		{"SetUint64", "ID", uint64(12345), uint64(12345), false},
		{"SetString", "Name", "test", "test", false},
		{"SetInt32", "Age", int32(25), int32(25), false},
		{"SetFloat64", "Score", float64(98.5), float64(98.5), false},
		{"SetBool", "Active", true, true, false},
		{"SetBytes", "Data", []byte("hello"), []byte("hello"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !tt.shouldPanic {
						t.Errorf("Unexpected panic: %v", r)
					}
				}
			}()

			ts := &TestStruct{}
			structType := reflect.TypeOf(*ts)
			field, found := structType.FieldByName(tt.fieldName)
			if !found {
				t.Fatalf("Field %s not found", tt.fieldName)
			}

			setter := createDirectSetterFunc(field.Offset, field.Type, nil)
			setter(unsafe.Pointer(ts), tt.value)

			// Verify the value was set correctly
			structValue := reflect.ValueOf(ts).Elem()
			fieldValue := structValue.FieldByName(tt.fieldName)
			actual := fieldValue.Interface()

			if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, actual)
			}
		})
	}
}

// TestMemoryBounds tests that we don't write outside struct boundaries
func TestMemoryBounds(t *testing.T) {
	t.Run("ValidOffsets", func(t *testing.T) {
		ts := &TestStruct{}
		structType := reflect.TypeOf(*ts)
		structSize := structType.Size()

		// Test all fields have valid offsets
		for i := 0; i < structType.NumField(); i++ {
			field := structType.Field(i)

			if field.Offset >= structSize {
				t.Errorf("Field %s offset %d exceeds struct size %d",
					field.Name, field.Offset, structSize)
			}

			if field.Offset+field.Type.Size() > structSize {
				t.Errorf("Field %s extends beyond struct bounds", field.Name)
			}
		}
	})

	t.Run("MemoryIntegrity", func(t *testing.T) {
		// Create a struct with sentinel values around target field
		type BoundedStruct struct {
			Sentinel1 uint64 // Should remain unchanged
			Target    uint64 // This is what we'll modify
			Sentinel2 uint64 // Should remain unchanged
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

		structType := reflect.TypeOf(*bs)
		field, _ := structType.FieldByName("Target")
		setter := createDirectSetterFunc(field.Offset, field.Type, nil)

		// Set the target field
		setter(unsafe.Pointer(bs), targetValue)

		// Verify sentinels are unchanged
		if bs.Sentinel1 != sentinel1Value {
			t.Errorf("Sentinel1 corrupted: expected %#x, got %#x",
				sentinel1Value, bs.Sentinel1)
		}
		if bs.Sentinel2 != sentinel2Value {
			t.Errorf("Sentinel2 corrupted: expected %#x, got %#x",
				sentinel2Value, bs.Sentinel2)
		}
		if bs.Target != targetValue {
			t.Errorf("Target not set correctly: expected %#x, got %#x",
				targetValue, bs.Target)
		}
	})
}

// TestConcurrentAccess tests thread safety
func TestConcurrentAccess(t *testing.T) {
	const numGoroutines = 100
	const numIterations = 1000

	structType := reflect.TypeOf(TestStruct{})
	field, _ := structType.FieldByName("ID")
	setter := createDirectSetterFunc(field.Offset, field.Type, nil)

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	// Launch multiple goroutines, each with its own struct
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			// Each goroutine gets its own struct - no race condition
			ts := &TestStruct{}

			defer func() {
				if r := recover(); r != nil {
					errors <- fmt.Errorf("goroutine %d panicked: %v", goroutineID, r)
				}
			}()

			for j := 0; j < numIterations; j++ {
				value := uint64(goroutineID*numIterations + j)
				setter(unsafe.Pointer(ts), value)

				// Small delay to increase chance of race conditions
				if j%100 == 0 {
					runtime.Gosched()
				}
			}

			// Verify final value for this goroutine's struct
			expectedFinal := uint64(goroutineID*numIterations + (numIterations - 1))
			if ts.ID != expectedFinal {
				errors <- fmt.Errorf("goroutine %d: expected final ID %d, got %d", goroutineID, expectedFinal, ts.ID)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}

	t.Logf("Concurrent test completed successfully with %d goroutines", numGoroutines)
}

// TestTypeConversions tests the conversion safety
func TestTypeConversions(t *testing.T) {
	int64Type := reflect.TypeOf(int64(0))
	stringType := reflect.TypeOf("")

	tests := []struct {
		name      string
		fieldType reflect.Type
		dbType    *reflect.Type
		input     any
		expectErr bool
	}{
		{
			name:      "Int64ToUint64Valid",
			fieldType: reflect.TypeOf(uint64(0)),
			dbType:    &int64Type,
			input:     int64(123),
			expectErr: false,
		},
		{
			name:      "Int64ToUint64Negative",
			fieldType: reflect.TypeOf(uint64(0)),
			dbType:    &int64Type,
			input:     int64(-123),
			expectErr: true, // Should set zero value due to conversion error
		},
		{
			name:      "StringToTime",
			fieldType: reflect.TypeOf(time.Time{}),
			dbType:    &stringType,
			input:     "2025-07-30T18:53:28Z",
			expectErr: false,
		},
		{
			name:      "InvalidTimeString",
			fieldType: reflect.TypeOf(time.Time{}),
			dbType:    &stringType,
			input:     "invalid-time",
			expectErr: true, // Should set zero value due to conversion error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a struct with just this field type for testing
			structType := reflect.StructOf([]reflect.StructField{
				{
					Name: "TestField",
					Type: tt.fieldType,
					Tag:  "",
				},
			})

			structPtr := reflect.New(structType)
			field := structType.Field(0)

			setter := createDirectSetterFunc(field.Offset, field.Type, tt.dbType)

			// Set the value (should not panic, but may set zero value on error)
			setter(unsafe.Pointer(structPtr.Pointer()), tt.input)

			// Check the result
			resultField := structPtr.Elem().Field(0)
			result := resultField.Interface()

			if tt.expectErr {
				// Should be zero value due to conversion error
				zeroValue := reflect.Zero(tt.fieldType).Interface()
				if !reflect.DeepEqual(result, zeroValue) {
					t.Errorf("Expected zero value %v for failed conversion, got %v", zeroValue, result)
				}
			} else {
				// Should not be zero value for successful conversion
				zeroValue := reflect.Zero(tt.fieldType).Interface()
				if reflect.DeepEqual(result, zeroValue) && tt.input != nil {
					t.Errorf("Expected non-zero value for successful conversion, got zero value")
				}
			}
		})
	}
}

// TestGarbageCollectorInteraction tests GC safety
func TestGarbageCollectorInteraction(t *testing.T) {
	const numStructs = 1000

	// Create many structs to trigger GC
	structs := make([]*TestStruct, numStructs)
	setters := make([]func(unsafe.Pointer, any), numStructs)

	for i := 0; i < numStructs; i++ {
		structs[i] = &TestStruct{}
		structType := reflect.TypeOf(*structs[i])
		field, _ := structType.FieldByName("ID")
		setters[i] = createDirectSetterFunc(field.Offset, field.Type, nil)
	}

	// Force garbage collection
	runtime.GC()
	runtime.GC()

	// Try to use the setters after GC
	for i := 0; i < numStructs; i++ {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Struct %d caused panic after GC: %v", i, r)
				}
			}()

			setters[i](unsafe.Pointer(structs[i]), uint64(i))

			if structs[i].ID != uint64(i) {
				t.Errorf("Struct %d: expected ID %d, got %d", i, i, structs[i].ID)
			}
		}()
	}
}

// TestAlignmentRequirements tests platform alignment safety
func TestAlignmentRequirements(t *testing.T) {
	// Test that our field offsets respect platform alignment requirements
	structType := reflect.TypeOf(TestStruct{})

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)

		// Check if offset is properly aligned for the field type
		requiredAlignment := field.Type.Align()
		if field.Offset%uintptr(requiredAlignment) != 0 {
			t.Errorf("Field %s at offset %d is not properly aligned (requires %d-byte alignment)",
				field.Name, field.Offset, requiredAlignment)
		}
	}
}

// TestNullPointerSafety tests handling of nil pointers
func TestNullPointerSafety(t *testing.T) {
	structType := reflect.TypeOf(TestStruct{})
	field, _ := structType.FieldByName("ID")
	setter := createDirectSetterFunc(field.Offset, field.Type, nil)

	// This should panic or be handled gracefully
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic when using nil pointer, but got none")
		}
	}()

	setter(nil, uint64(123))
}

// TestLargeStructPerformance tests performance with large structs
func TestLargeStructPerformance(t *testing.T) {
	ls := &LargeStruct{}
	structType := reflect.TypeOf(*ls)
	field, _ := structType.FieldByName("Field3")
	setter := createDirectSetterFunc(field.Offset, field.Type, nil)

	start := time.Now()

	// Perform many operations
	for i := 0; i < 100000; i++ {
		setter(unsafe.Pointer(ls), uint64(i))
	}

	duration := time.Since(start)
	t.Logf("100k operations on large struct took: %v", duration)

	if ls.Field3 != 99999 {
		t.Errorf("Expected Field3 to be 99999, got %d", ls.Field3)
	}
}

// BenchmarkDirectSetterVsReflection compares performance
func BenchmarkDirectSetterVsReflection(b *testing.B) {
	ts := &TestStruct{}
	structType := reflect.TypeOf(*ts)
	field, _ := structType.FieldByName("ID")

	// Setup direct setter
	directSetter := createDirectSetterFunc(field.Offset, field.Type, nil)

	// Setup reflection setter
	reflectionSetter := func(structPtr *TestStruct, value uint64) {
		reflect.ValueOf(structPtr).Elem().FieldByName("ID").SetUint(value)
	}

	b.Run("DirectSetter", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			directSetter(unsafe.Pointer(ts), uint64(i))
		}
	})

	b.Run("Reflection", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			reflectionSetter(ts, uint64(i))
		}
	})
}

// TestMemoryLeaks tests for potential memory leaks
func TestMemoryLeaks(t *testing.T) {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	// Create and use many setters
	for i := 0; i < 1000; i++ { // Reduced iterations
		ts := &TestStruct{}
		structType := reflect.TypeOf(*ts)
		field, _ := structType.FieldByName("Name")
		setter := createDirectSetterFunc(field.Offset, field.Type, nil)
		setter(unsafe.Pointer(ts), fmt.Sprintf("test-%d", i))
	}

	runtime.GC()
	runtime.GC() // Double GC to ensure cleanup
	runtime.ReadMemStats(&m2)

	// Calculate memory growth more safely
	var memGrowth uint64
	if m2.Alloc > m1.Alloc {
		memGrowth = m2.Alloc - m1.Alloc
	} else {
		memGrowth = 0 // Handle wraparound case
	}

	t.Logf("Memory growth: %d bytes (before: %d, after: %d)", memGrowth, m1.Alloc, m2.Alloc)

	// More reasonable threshold for fewer iterations
	if memGrowth > 512*1024 { // 512KB threshold
		t.Errorf("Potential memory leak detected: %d bytes growth", memGrowth)
	}
}
