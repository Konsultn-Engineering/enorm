//go:build !go1.22
// +build !go1.22

package schema

var legacyCache sync.Map // map[reflect.Type]*EntityMeta

func Introspect(t reflect.Type) (*EntityMeta, error) {
	t = indirectType(t)
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("invalid model type: %s", t.Kind())
	}

	if meta, ok := legacyCache.Load(t); ok {
		return meta.(*EntityMeta), nil
	}

	meta, err := buildMeta(t)
	if err != nil {
		return nil, err
	}
	legacyCache.Store(t, meta)
	return meta, nil
}
