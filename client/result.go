package client

type result struct {
	affectedRows int64
	lastInsertId int64
}

func (r *result) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r *result) RowsAffected() (int64, error) {
	return r.affectedRows, nil
}
