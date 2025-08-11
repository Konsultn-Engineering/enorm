package query

type Condition struct {
}

// Multiple records with conditions
func FindBy[T any](conditions ...Condition) ([]*T, error) {
	return nil, nil
}

// Single record with conditions
func FindOneBy[T any](conditions ...Condition) (*T, error) {
	return nil, nil
}

// Check if record exists
func Exists[T any](conditions ...Condition) (bool, error) {
	return false, nil
}

// Count records
func Count[T any](conditions ...Condition) (int64, error) {
	return 0, nil
}

// Find by primary key
func FindByID[T any](id interface{}) (*T, error) {
	return nil, nil
}

func CreateMany[T any](items []*T) error {
	return nil
}

// Bulk update with conditions
func UpdateWhere[T any](updates map[string]interface{}, conditions ...Condition) (int64, error) {
	return 0, nil
}

// Bulk delete with conditions
func DeleteWhere[T any](conditions ...Condition) (int64, error) {
	return 0, nil
}

// Upsert (insert or update)
func Upsert[T any](item *T, conflictColumns ...string) error {
	return nil
}

func FindWithPagination[T any](page, limit int, conditions ...Condition) ([]*T, int64, error) {
	return nil, 0, nil
}

// Find first record
func First[T any](conditions ...Condition) (*T, error) {
	return nil, nil
}

// Find last record
func Last[T any](conditions ...Condition) (*T, error) {
	return nil, nil
}

// Find with ordering
func FindOrdered[T any](orderBy string, conditions ...Condition) ([]*T, error) {
	return nil, nil
}

// Find with limit
func FindLimit[T any](limit int, conditions ...Condition) ([]*T, error) {
	return nil, nil
}

func Max[T any](column string, conditions ...Condition) (interface{}, error) {
	return nil, nil
}

// Get minimum value of a column
func Min[T any](column string, conditions ...Condition) (interface{}, error) {
	return nil, nil
}

// Get average value of a column
func Avg[T any](column string, conditions ...Condition) (float64, error) {
	return 0, nil
}

// Get sum of a column
func Sum[T any](column string, conditions ...Condition) (interface{}, error) {
	return nil, nil
}
func WithTransaction[T any](fn func() error) error {
	return nil
}

// Get raw database connection for custom queries
func Raw[T any](query string, args ...interface{}) ([]*T, error) {
	return nil, nil
}

func RawOne[T any](query string, args ...interface{}) (*T, error) {
	return nil, nil
}

func SoftDelete[T any](item *T) error {
	return nil
}

// Restore soft deleted record
func Restore[T any](item *T) error {
	return nil
}

// Force delete (bypass soft delete)
func ForceDelete[T any](item *T) error {
	return nil
}

// Reload record from database
func Reload[T any](item *T) error {
	return nil
}
