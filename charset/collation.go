package charset

// https://dev.mysql.com/doc/internals/en/character-set.html#packet-Protocol::CharacterSet
// TODO more collations

const (
	UTF8GeneralCi    = "utf8_general_ci"
	UTF8MB4GeneralCi = "utf8mb4_general_ci"
	UTF8MB40900AiCi  = "utf8mb4_0900_ai_ci"
)

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
