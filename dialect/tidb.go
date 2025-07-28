package dialect

type TiDB struct {
	*MySQL
}

func NewTiDBDialect() Dialect {
	return &TiDB{
		MySQL: NewMySQLDialect().(*MySQL),
	}
}

func (t *TiDB) SupportsVector() bool {
	return true
}
