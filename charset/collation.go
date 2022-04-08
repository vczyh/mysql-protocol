package charset

import "fmt"

// https://dev.mysql.com/doc/internals/en/character-set.html#packet-Protocol::CharacterSet
// TODO more collations

const (
	UTF8    = "utf8"
	UTF8MB4 = "utf8mb4"
	Binary  = "binary"

	UTF8GeneralCi    = "utf8_general_ci"
	UTF8MB4GeneralCi = "utf8mb4_general_ci"
	UTF8MB40900AiCi  = "utf8mb4_0900_ai_ci"
)

var (
	collations = []*Collation{
		{UTF8, true, 33, UTF8GeneralCi},
		{UTF8MB4, false, 45, UTF8MB4GeneralCi},
		{UTF8MB4, true, 255, UTF8MB40900AiCi},
		{Binary, true, 63, Binary},
	}

	//CollationUtf8GeneralCi    = Collation{33, charsetNameUtf8, "utf8_general_ci"}
	//CollationUtf8mb4GeneralCi = Collation{45, charsetNameUtf8mb4, "utf8mb4_general_ci"}
	//CollationUtf8mb40900AiCi  = Collation{255, charsetNameUtf8mb4, "utf8mb4_0900_ai_ci"}
	//CollationBinary           = Collation{63, charsetNameBinary, "binary"}

	collationNameMap = map[string]*Collation{
		//CollationUtf8GeneralCi.name:    &CollationUtf8GeneralCi,
		//CollationUtf8mb4GeneralCi.name: &CollationUtf8mb4GeneralCi,
		//CollationUtf8mb40900AiCi.name:  &CollationUtf8mb40900AiCi,
		//CollationBinary.name:           &CollationBinary,
	}

	collationIdMap = map[uint64]*Collation{
		//CollationUtf8GeneralCi.id:    &CollationUtf8GeneralCi,
		//CollationUtf8mb4GeneralCi.id: &CollationUtf8mb4GeneralCi,
		//CollationUtf8mb40900AiCi.id:  &CollationUtf8mb40900AiCi,
		//CollationBinary.id:           &CollationBinary,
	}

	charsetMap = map[string]*Charset{
		//Utf8.name:    &Utf8,
		//Utf8mb4.name: &Utf8mb4,
		//Binary.name:  &Binary,
	}
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

type Collation struct {
	charset   string
	isDefault bool
	id        uint64
	name      string
}

func (c *Collation) Id() uint64 {
	return c.id
}

func (c *Collation) Name() string {
	return c.name
}

func (c *Collation) Charset() *Charset {
	return charsetMap[c.charset]
}

var (
//Utf8    = Charset{charsetNameUtf8, CollationUtf8GeneralCi}
//Utf8mb4 = Charset{charsetNameUtf8mb4, CollationUtf8mb40900AiCi}
//Binary  = Charset{charsetNameBinary, CollationBinary}

//charsets = map[string]*Charset{
//Utf8.name:    &Utf8,
//Utf8mb4.name: &Utf8mb4,
//Binary.name:  &Binary,
//}
)

type Charset struct {
	name             string
	defaultCollation *Collation
}

func (c *Charset) Name() string {
	return c.name
}

func (c *Charset) DefaultCollation() *Collation {
	return c.defaultCollation
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

//func GetCollationCharset(id uint8) (Charset, error) {
//	collation, err := GetCollation(id)
//	if err != nil {
//		return Charset{}, err
//	}
//	return Get(collation.charset)
//}

func Get(name string) (*Charset, error) {
	charset, ok := charsetMap[name]
	if !ok {
		return nil, fmt.Errorf("charset name not found")
	}
	return charset, nil
}
