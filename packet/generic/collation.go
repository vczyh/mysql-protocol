package generic

var Collations = map[string]*Collation{
	"utf8_general_ci":    Utf8GeneralCi,
	"utf8mb4_general_ci": Utf8mb4GeneralCi,
	"utf8mb4_0900_ai_ci": Utf8mb40900AiCi,
	"binary":             Binary,
}

var CollationIds = map[uint8]*Collation{
	33:  Utf8GeneralCi,
	45:  Utf8mb4GeneralCi,
	255: Utf8mb40900AiCi,
	63:  Binary,
}

// TODO more collations
// https://dev.mysql.com/doc/internals/en/character-set.html#packet-Protocol::CharacterSet
var (
	Utf8GeneralCi    = &Collation{33, "utf8", "utf8_general_ci"}
	Utf8mb4GeneralCi = &Collation{45, "utf8mb4", "utf8mb4_general_ci"}
	Utf8mb40900AiCi  = &Collation{255, "utf8mb4", "utf8mb4_0900_ai_ci"}
	Binary           = &Collation{63, "binary", "binary"}
)

type Collation struct {
	Id            uint8
	CharSetName   string
	CollationName string
}
