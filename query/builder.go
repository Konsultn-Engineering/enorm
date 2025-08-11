package query

type Builder struct {
	selectBuilder SelectBuilder
}

func NewBuilder() Builder {
	return Builder{
		selectBuilder: *selectBuilderPool.Get().(*SelectBuilder),
	}
}

func (b Builder) Select() *SelectBuilder {
	return &b.selectBuilder
}

func (b Builder) Where(field string, value interface{}) *SelectBuilder {
	return &b.selectBuilder
}

func (b Builder) Update() bool {
	return true
}

func (b Builder) Updates(map[string]any) bool {
	return true
}

func (b Builder) Upsert() bool {
	return true
}

func (b Builder) Delete() bool {
	return true
}

func (b Builder) Create() bool {
	return true
}
