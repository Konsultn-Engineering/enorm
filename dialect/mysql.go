package dialect

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type MySQL struct{}

func NewMySQLDialect() Dialect {
	return &MySQL{}
}

func (m MySQL) QuoteIdentifier(name string) string {
	return "`" + name + "`"
}

func (m MySQL) Placeholder(n int) string {
	return "?"
}

func (m MySQL) RenderValue(v any) string {
	switch val := v.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32, float64:
		return strconv.FormatFloat(reflect.ValueOf(val).Float(), 'f', -1, 64)
	case time.Time:
		return "'" + val.Format("2006-01-02 15:04:05.000000") + "'"
	case []byte:
		return fmt.Sprintf("E'\\\\x%x'", val) // hex bytea literal
	default:
		return "'" + strings.ReplaceAll(fmt.Sprint(val), "'", "''") + "'"
	}
}

func (m MySQL) SupportsVector() bool {
	return false
}
