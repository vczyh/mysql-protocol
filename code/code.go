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
)

// 2,000 to 2,999: Client error codes reserved for use by the client library.
const ()

// 3,000 to 4,999: Server error codes reserved for messages sent to clients.
const ()

// 5,000 to 5,999: Error codes reserved for use by X Plugin for messages sent to clients.
const ()

// 10,000 to 49,999: Server error codes reserved for messages to be written to the error log (not sent to clients).
// When an error in this range occurs, the server writes it to the error log, padding the error code with leading
// zeros to six digits and adding a prefix of MY-.
const ()

// 50,000 to 51,999: Error codes reserved for use by third parties (not sent to clients).
const (
	ErrGeneral Code = 50000
)
