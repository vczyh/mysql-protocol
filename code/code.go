package code

type Code uint16

// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
// https://dev.mysql.com/doc/refman/8.0/en/error-message-elements.html

// 1 to 999: Global error codes.
// This error code range is called “global” because it is a shared range
// that is used by the server as well as by clients.
//
// When an error in this range originates on the server side, the server writes it to the error log,
// padding the error code with leading zeros to six digits and adding a prefix of MY-.
//
// When an error in this range originates on the client side, the client library makes it available to
// the client program with no zero-padding or prefix.
const ()

// 1,000 to 1,999: Server error codes reserved for messages sent to clients.
const (
	ErrNo                Code = 1002
	ErrYes               Code = 1003
	ErrAccessDeniedError Code = 1045

	Err Code = 50000
)
