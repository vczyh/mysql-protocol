package flag

// Capability Flags: https://dev.mysql.com/doc/internals/en/capability-flags.html
const (
	ClientLongPassword CapabilityFlag = 1 << iota
	ClientFoundRows
	ClientLongFlag
	ClientConnectWithDB
	ClientNoSchema
	ClientCompress
	ClientODBC
	ClientLocalFiles
	ClientIgnoreSpace
	ClientProtocol41
	ClientInteractive
	ClientSSL
	ClientIgnoreSigpipe
	ClientTransactions
	ClientReserved
	ClientSecureConnection
	ClientMultiStatements
	ClientMultiResults
	ClientPsMultiResults
	ClientPluginAuth
	ClientConnectAttrs
	ClientPluginAuthLenencClientData
	ClientCanHandleExpiredPasswords
	ClientSessionTrack
	ClientDeprecateEOF
)

func (c CapabilityFlag) String() string {
	switch c {
	case ClientLongPassword:
		return "CLIENT_LONG_PASSWORD"
	case ClientFoundRows:
		return "CLIENT_FOUND_ROWS"
	case ClientLongFlag:
		return "CLIENT_LONG_FLAG"
	case ClientConnectWithDB:
		return "CLIENT_CONNECT_WITH_DB"
	case ClientNoSchema:
		return "CLIENT_NO_SCHEMA"
	case ClientCompress:
		return "CLIENT_COMPRESS"
	case ClientODBC:
		return "CLIENT_ODBC"
	case ClientLocalFiles:
		return "CLIENT_LOCAL_FILES"
	case ClientIgnoreSpace:
		return "CLIENT_IGNORE_SPACE"
	case ClientProtocol41:
		return "CLIENT_PROTOCOL_41"
	case ClientInteractive:
		return "CLIENT_INTERACTIVE"
	case ClientSSL:
		return "CLIENT_SSL"
	case ClientIgnoreSigpipe:
		return "CLIENT_IGNORE_SIGPIPE"
	case ClientTransactions:
		return "CLIENT_TRANSACTIONS"
	case ClientReserved:
		return "CLIENT_RESERVED"
	case ClientSecureConnection:
		return "CLIENT_SECURE_CONNECTION"
	case ClientMultiStatements:
		return "CLIENT_MULTI_STATEMENTS"
	case ClientMultiResults:
		return "CLIENT_MULTI_RESULTS"
	case ClientPsMultiResults:
		return "CLIENT_PS_MULTI_RESULTS"
	case ClientPluginAuth:
		return "CLIENT_PLUGIN_AUTH"
	case ClientConnectAttrs:
		return "CLIENT_CONNECT_ATTRS"
	case ClientPluginAuthLenencClientData:
		return "CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA"
	case ClientCanHandleExpiredPasswords:
		return "CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS"
	case ClientSessionTrack:
		return "CLIENT_SESSION_TRACK"
	case ClientDeprecateEOF:
		return "CLIENT_DEPRECATE_EOF"
	default:
		return "Unknown CapabilityFlag"
	}
}
