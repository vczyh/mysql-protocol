package charset

const (
	UTF8    = "utf8"
	UTF8MB4 = "utf8mb4"
	Binary  = "binary"
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
