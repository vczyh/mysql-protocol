package client

//type subConn struct {
//	conn net.Conn
//}
//
//func (c *subConn) Next(n int) ([]byte, error) {
//	bs := make([]byte, n)
//	_, err := c.conn.Read(bs)
//	if err != nil {
//		return nil, err
//	}
//	return bs, nil
//}
//
//func (c *subConn) Write(data []byte) (int, error) {
//	return c.conn.Write(data)
//}
//
//func (c *subConn) close() error {
//	return c.conn.Close()
//}
