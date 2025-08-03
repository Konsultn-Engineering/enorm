package schema

import (
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"
)

// =========================================================================
// Benchmark Data Structures
// =========================================================================

type BenchUser struct {
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

type SmallBenchStruct struct {
	ID   uint64 `db:"id"`
	Name string `db:"name"`
}

type LargeBenchStruct struct {
	Field1  [100]byte `db:"field1"`
	Field2  string    `db:"field2"`
	Field3  uint64    `db:"field3"`
	Field4  [200]byte `db:"field4"`
	Field5  int32     `db:"field5"`
	Field6  float64   `db:"field6"`
	Field7  bool      `db:"field7"`
	Field8  time.Time `db:"field8"`
	Field9  []byte    `db:"field9"`
	Field10 string    `db:"field10"`
}

// =========================================================================
// Core Introspection Benchmarks
// =========================================================================

func BenchmarkIntrospect(b *testing.B) {
	userType := reflect.TypeOf(BenchUser{})

	b.Run("ColdStart", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ClearCache()
			ClearPrecompiled()
			_, err := Introspect(userType)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("CachedAccess", func(b *testing.B) {
		// Warm up cache
		Introspect(userType)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := Introspect(userType)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("PrecompiledAccess", func(b *testing.B) {
		// Precompile type
		PrecompileType[BenchUser]()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := Introspect(userType)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkIntrospectBySize(b *testing.B) {
	b.Run("SmallStruct", func(b *testing.B) {
		smallType := reflect.TypeOf(SmallBenchStruct{})
		PrecompileType[SmallBenchStruct]()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := Introspect(smallType)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("MediumStruct", func(b *testing.B) {
		userType := reflect.TypeOf(BenchUser{})
		PrecompileType[BenchUser]()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := Introspect(userType)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("LargeStruct", func(b *testing.B) {
		largeType := reflect.TypeOf(LargeBenchStruct{})
		PrecompileType[LargeBenchStruct]()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := Introspect(largeType)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkIntrospectConcurrent(b *testing.B) {
	userType := reflect.TypeOf(BenchUser{})
	PrecompileType[BenchUser]()

	b.Run("Sequential", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := Introspect(userType)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, err := Introspect(userType)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

// =========================================================================
// Direct Setter Benchmarks
// =========================================================================

func BenchmarkDirectSetters(b *testing.B) {
	user := &BenchUser{}
	meta, err := Introspect(reflect.TypeOf(user))
	if err != nil {
		b.Fatal(err)
	}

	b.Run("SetUint64", func(b *testing.B) {
		setter := meta.ColumnMap["id"].DirectSet
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			setter(unsafe.Pointer(user), uint64(i))
		}
	})

	b.Run("SetString", func(b *testing.B) {
		setter := meta.ColumnMap["first_name"].DirectSet
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			setter(unsafe.Pointer(user), "John")
		}
	})

	b.Run("SetInt32", func(b *testing.B) {
		setter := meta.ColumnMap["age"].DirectSet
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			setter(unsafe.Pointer(user), int32(25))
		}
	})

	b.Run("SetFloat64", func(b *testing.B) {
		setter := meta.ColumnMap["score"].DirectSet
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			setter(unsafe.Pointer(user), float64(95.5))
		}
	})

	b.Run("SetBool", func(b *testing.B) {
		setter := meta.ColumnMap["active"].DirectSet
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			setter(unsafe.Pointer(user), true)
		}
	})

	b.Run("SetTime", func(b *testing.B) {
		setter := meta.ColumnMap["created_at"].DirectSet
		now := time.Now()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			setter(unsafe.Pointer(user), now)
		}
	})

	b.Run("SetBytes", func(b *testing.B) {
		setter := meta.ColumnMap["data"].DirectSet
		data := []byte("test data")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			setter(unsafe.Pointer(user), data)
		}
	})
}

func BenchmarkDirectSetterVsReflection(b *testing.B) {
	user := &BenchUser{}
	meta, err := Introspect(reflect.TypeOf(user))
	if err != nil {
		b.Fatal(err)
	}

	directSetter := meta.ColumnMap["id"].DirectSet
	reflectValue := reflect.ValueOf(user).Elem().FieldByName("ID")

	b.Run("DirectSetter", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			directSetter(unsafe.Pointer(user), uint64(i))
		}
	})

	b.Run("ReflectionSetter", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reflectValue.SetUint(uint64(i))
		}
	})
}

func BenchmarkDirectSettersConcurrent(b *testing.B) {
	meta, err := Introspect(reflect.TypeOf(BenchUser{}))
	if err != nil {
		b.Fatal(err)
	}

	setter := meta.ColumnMap["id"].DirectSet

	b.Run("Sequential", func(b *testing.B) {
		user := &BenchUser{}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			setter(unsafe.Pointer(user), uint64(i))
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			user := &BenchUser{} // Each goroutine gets its own user
			i := uint64(0)
			for pb.Next() {
				setter(unsafe.Pointer(user), i)
				i++
			}
		})
	})
}

// =========================================================================
// Field Binding Benchmarks
// =========================================================================

func BenchmarkFieldBinding(b *testing.B) {
	user := &BenchUser{}

	b.Run("CreateBinder", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			binder := newBinder(user)
			returnBinder(binder)
		}
	})

	b.Run("BindFields", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			binder := newBinder(user) // This already clears bindings
			err := binder.Bind(user, &user.ID, &user.FirstName, &user.Email, &user.Age)
			if err != nil {
				b.Fatal(err)
			}
			returnBinder(binder)
		}
	})

	b.Run("BindAllFields", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			binder := newBinder(user) // This already clears bindings
			err := binder.Bind(user,
				&user.ID, &user.FirstName, &user.LastName, &user.Email, &user.Age,
				&user.Score, &user.Active, &user.CreatedAt, &user.UpdatedAt, &user.Data)
			if err != nil {
				b.Fatal(err)
			}
			returnBinder(binder)
		}
	})

	b.Run("GetBindings", func(b *testing.B) {
		binder := newBinder(user)
		defer returnBinder(binder)

		binder.Bind(user, &user.ID, &user.FirstName, &user.Email)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = binder.Bindings()
		}
	})
}
func BenchmarkFieldBindingPool(b *testing.B) {
	user := &BenchUser{}

	b.Run("WithPool", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			binder := newBinder(user)
			binder.Bind(user, &user.ID, &user.FirstName)
			returnBinder(binder)
		}
	})

	b.Run("WithoutPool", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Simulate creating new binder each time (no pooling)
			binder := &fieldBinder{
				bindings: make(map[string]func(any, any)),
			}
			// Simulate binding work
			_ = binder
		}
	})
}

// =========================================================================
// Type Conversion Benchmarks
// =========================================================================

func BenchmarkTypeConversions(b *testing.B) {
	b.Run("Int64ToUint64", func(b *testing.B) {
		converter, err := GetConverter(uint64(0), reflect.TypeOf(int64(0)))
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = converter(int64(42))
		}
	})

	b.Run("StringToTime", func(b *testing.B) {
		converter, err := GetConverter(time.Time{}, reflect.TypeOf(""))
		if err != nil {
			b.Fatal(err)
		}

		timeStr := "2025-07-31T21:47:02Z"
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = converter(timeStr)
		}
	})

	b.Run("StringToString", func(b *testing.B) {
		converter, err := GetConverter("", reflect.TypeOf(""))
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = converter("test string")
		}
	})

	b.Run("IntToInt", func(b *testing.B) {
		converter, err := GetConverter(int(0), reflect.TypeOf(int(0)))
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = converter(42)
		}
	})
}

// =========================================================================
// Cache Performance Benchmarks
// =========================================================================

func BenchmarkCachePerformance(b *testing.B) {
	types := []reflect.Type{
		reflect.TypeOf(BenchUser{}),
		reflect.TypeOf(SmallBenchStruct{}),
		reflect.TypeOf(LargeBenchStruct{}),
	}

	b.Run("CacheHit", func(b *testing.B) {
		// Warm up cache
		for _, t := range types {
			Introspect(t)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			t := types[i%len(types)]
			_, err := Introspect(t)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("CacheMiss", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ClearCache()
			_, err := Introspect(reflect.TypeOf(BenchUser{}))
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("PrecompiledHit", func(b *testing.B) {
		// Precompile types
		PrecompileType[BenchUser]()
		PrecompileType[SmallBenchStruct]()
		PrecompileType[LargeBenchStruct]()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			t := types[i%len(types)]
			_, err := Introspect(t)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCacheConcurrency(b *testing.B) {
	userType := reflect.TypeOf(BenchUser{})

	b.Run("ConcurrentCacheAccess", func(b *testing.B) {
		// Warm up cache
		Introspect(userType)

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, err := Introspect(userType)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	b.Run("ConcurrentPrecompiledAccess", func(b *testing.B) {
		PrecompileType[BenchUser]()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, err := Introspect(userType)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

// =========================================================================
// Memory Allocation Benchmarks
// =========================================================================

func BenchmarkMemoryAllocations(b *testing.B) {
	user := &BenchUser{}
	meta, _ := Introspect(reflect.TypeOf(user))

	b.Run("IntrospectAllocations", func(b *testing.B) {
		userType := reflect.TypeOf(BenchUser{})
		PrecompileType[BenchUser]() // Use precompiled to minimize allocations

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := Introspect(userType)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("DirectSetterAllocations", func(b *testing.B) {
		setter := meta.ColumnMap["id"].DirectSet

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			setter(unsafe.Pointer(user), uint64(i))
		}
	})

	b.Run("BinderAllocations", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			binder := newBinder(user)
			binder.Bind(user, &user.ID, &user.FirstName)
			returnBinder(binder)
		}
	})
}

// =========================================================================
// Throughput Benchmarks
// =========================================================================

func BenchmarkThroughput(b *testing.B) {
	const numOperations = 1000000

	b.Run("IntrospectThroughput", func(b *testing.B) {
		userType := reflect.TypeOf(BenchUser{})
		PrecompileType[BenchUser]()

		start := time.Now()
		for i := 0; i < numOperations; i++ {
			Introspect(userType)
		}
		duration := time.Since(start)

		opsPerSec := float64(numOperations) / duration.Seconds()
		b.ReportMetric(opsPerSec, "ops/sec")
	})

	b.Run("DirectSetterThroughput", func(b *testing.B) {
		user := &BenchUser{}
		meta, _ := Introspect(reflect.TypeOf(user))
		setter := meta.ColumnMap["id"].DirectSet

		start := time.Now()
		for i := 0; i < numOperations; i++ {
			setter(unsafe.Pointer(user), uint64(i))
		}
		duration := time.Since(start)

		opsPerSec := float64(numOperations) / duration.Seconds()
		b.ReportMetric(opsPerSec, "ops/sec")
	})
}

// =========================================================================
// Stress Tests
// =========================================================================

func BenchmarkStressTest(b *testing.B) {
	const numGoroutines = 100
	const numOperations = 1000

	b.Run("ConcurrentIntrospect", func(b *testing.B) {
		userType := reflect.TypeOf(BenchUser{})
		PrecompileType[BenchUser]()

		b.ResetTimer()

		var wg sync.WaitGroup
		start := time.Now()

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					_, err := Introspect(userType)
					if err != nil {
						b.Error(err)
						return
					}
				}
			}()
		}

		wg.Wait()
		duration := time.Since(start)

		totalOps := numGoroutines * numOperations
		opsPerSec := float64(totalOps) / duration.Seconds()
		b.ReportMetric(opsPerSec, "ops/sec")
	})

	b.Run("ConcurrentDirectSetters", func(b *testing.B) {
		meta, _ := Introspect(reflect.TypeOf(BenchUser{}))
		setter := meta.ColumnMap["id"].DirectSet

		b.ResetTimer()

		var wg sync.WaitGroup
		start := time.Now()

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				user := &BenchUser{} // Each goroutine gets its own user

				for j := 0; j < numOperations; j++ {
					value := uint64(goroutineID*numOperations + j)
					setter(unsafe.Pointer(user), value)
				}
			}(i)
		}

		wg.Wait()
		duration := time.Since(start)

		totalOps := numGoroutines * numOperations
		opsPerSec := float64(totalOps) / duration.Seconds()
		b.ReportMetric(opsPerSec, "ops/sec")
	})
}

// =========================================================================
// Comparison Benchmarks
// =========================================================================

func BenchmarkComparisons(b *testing.B) {
	user := &BenchUser{}

	b.Run("DirectVsReflectFieldAccess", func(b *testing.B) {
		meta, _ := Introspect(reflect.TypeOf(user))
		setter := meta.ColumnMap["id"].DirectSet
		value := reflect.ValueOf(user).Elem().FieldByName("ID")

		b.Run("Direct", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				setter(unsafe.Pointer(user), uint64(i))
			}
		})

		b.Run("Reflection", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				value.SetUint(uint64(i))
			}
		})
	})

	b.Run("PrecompiledVsCached", func(b *testing.B) {
		userType := reflect.TypeOf(BenchUser{})

		b.Run("Precompiled", func(b *testing.B) {
			PrecompileType[BenchUser]()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := Introspect(userType)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run("Cached", func(b *testing.B) {
			ClearPrecompiled()
			Introspect(userType) // Warm cache
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := Introspect(userType)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

// =========================================================================
// Benchmark Utilities
// =========================================================================

func BenchmarkSetup(b *testing.B) {
	// Benchmark the setup/teardown operations themselves

	b.Run("CacheInitialization", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			InitializeCache(256, nil)
		}
	})

	b.Run("PrecompileType", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ClearPrecompiled()
			PrecompileType[BenchUser]()
		}
	})

	b.Run("CacheClear", func(b *testing.B) {
		// Warm up cache first
		Introspect(reflect.TypeOf(BenchUser{}))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ClearCache()
		}
	})
}
