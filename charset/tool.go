package charset

import "fmt"

func Get(name string) (*Charset, error) {
	charset, ok := charsetMap[name]
	if !ok {
		return nil, fmt.Errorf("charset name not found")
	}
	return charset, nil
}

func GetCollation(id uint64) (*Collation, error) {
	collation, ok := collationIdMap[id]
	if !ok {
		return nil, fmt.Errorf("collation id not found")
	}
	return collation, nil
}

func GetCollationByName(name string) (*Collation, error) {
	collation, ok := collationNameMap[name]
	if !ok {
		return nil, fmt.Errorf("collation name not found")
	}
	return collation, nil
}

var (
	collations = []*Collation{
		{UTF8, true, 33, UTF8GeneralCi},
		{UTF8MB4, false, 45, UTF8MB4GeneralCi},
		{UTF8MB4, true, 255, UTF8MB40900AiCi},
		{Binary, true, 63, Binary},
	}

	collationNameMap = map[string]*Collation{}
	collationIdMap   = map[uint64]*Collation{}
	charsetMap       = map[string]*Charset{}
)

func init() {
	for _, collation := range collations {
		collationNameMap[collation.name] = collation
		collationIdMap[collation.id] = collation

		if collation.isDefault {
			charsetMap[collation.charset] = &Charset{
				name:             collation.charset,
				defaultCollation: collation,
			}
		}
	}
}
