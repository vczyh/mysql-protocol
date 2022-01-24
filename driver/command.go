package driver

//// Quit is implement of the COM_QUIT
//func (c *client.Conn) Quit() error {
//	pkt := command.NewQuit()
//	if err := c.writeCommandPacket(pkt); err != nil {
//		return err
//	}
//
//	data, err := c.readPacket()
//	// response is either a connection close or a OK_Packet
//	if err == nil && generic.IsOK(data) {
//		return nil
//	}
//	return err
//}
//
//// InitDB is implement of the COM_QUIT
//func (c *client.Conn) InitDB(db string) error {
//	pkt := command.NewInitDB(db)
//	if err := c.writeCommandPacket(pkt); err != nil {
//		return err
//	}
//	return c.readOKErrPacket()
//}
//
//
//// Query is implement of the COM_QUERY
//func (c *client.Conn) Query(query string) (driver.Rows, error) {
//	pkt := command.NewQuery(query)
//	if err := c.writeCommandPacket(pkt); err != nil {
//		return nil, err
//	}
//
//	columnCount, err := c.readExecuteResponseFirstPacket()
//	if err != nil {
//		return nil, err
//	}
//
//	rows := new(driver2.textRows)
//	rows.conn = c
//
//	if columnCount > 0 {
//		rows.columns, err = c.readColumns(columnCount)
//	} else {
//		// TODO done variable
//		// TODO 没有column 可能是update语句等
//	}
//
//	return rows, nil
//}
//
//// TODO MySQL 8.0.27 not work
//// CreateDB is implement of the COM_CREATE_DB
//func (c *client.Conn) CreateDB(db string) error {
//	pkt := command.NewCreateDB(db)
//	if err := c.writeCommandPacket(pkt); err != nil {
//		return err
//	}
//	return c.readOKErrPacket()
//}
//
//// TODO MySQL 8.0.27 not work
//// DropDB is implement of the COM_DROP_DB
//func (c *client.Conn) DropDB(db string) error {
//	pkt := command.NewDropDB(db)
//	if err := c.writeCommandPacket(pkt); err != nil {
//		return err
//	}
//	return c.readOKErrPacket()
//}
//
//// TODO MySQL 8.0.27 not work
//func (c *client.Conn) Shutdown() error {
//	pkt := command.NewShutdown()
//	if err := c.writeCommandPacket(pkt); err != nil {
//		return err
//	}
//
//	data, err := c.readPacket()
//	if err != nil {
//		return err
//	}
//	switch {
//	case generic.IsErr(data):
//		return c.handleOKErrPacket(data)
//	case generic.IsEOF(data):
//		return nil
//	default:
//		return generic.ErrPacketData
//	}
//}
//
//// Statistics is implement of the COM_CREATE_DB
//func (c *client.Conn) Statistics() (string, error) {
//	pkt := command.NewStatistics()
//	if err := c.writeCommandPacket(pkt); err != nil {
//		return "", err
//	}
//
//	data, err := c.readPacket()
//	if err != nil {
//		return "", err
//	}
//	switch {
//	case generic.IsErr(data):
//		return "", c.handleOKErrPacket(data)
//	default:
//		return string(data[4:]), nil
//	}
//}
//
//// TODO MySQL 8.0.27 not work
////func (c *Conn) ProcessInfo() (*ResultSet, error) {
////	pkt := command.NewProcessInfo()
////	if err := c.writeCommandPacket(pkt); err != nil {
////		return nil, err
////	}
////
////	data, err := c.readPacket()
////	if err != nil {
////		return nil, err
////	}
////	switch {
////	case generic.IsErr(data):
////		return nil, c.handleOKErrPacket(data)
////	default:
////		return c.handleResultSet(data)
////	}
////}
//
//// ProcessKill is implement of the COM_PROCESS_KILL
//func (c *client.Conn) ProcessKill(connectionId int) error {
//	pkt := command.NewProcessKill(connectionId)
//	if err := c.writeCommandPacket(pkt); err != nil {
//		return err
//	}
//	return c.readOKErrPacket()
//}
//
//// TODO 没报错没效果
//// General log
//// 2022-01-17T10:06:33.163277Z	   22 Debug
//func (c *client.Conn) Debug() error {
//	pkt := command.NewDebug()
//	if err := c.writeCommandPacket(pkt); err != nil {
//		return err
//	}
//
//	data, err := c.readPacket()
//	if err != nil {
//		return err
//	}
//	switch {
//	case generic.IsErr(data):
//		return c.handleOKErrPacket(data)
//	case generic.IsEOF(data):
//		return nil
//	default:
//		return generic.ErrPacketData
//	}
//}
