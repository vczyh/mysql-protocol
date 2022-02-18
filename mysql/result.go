package mysql

type Result struct {
	affectedRows uint64
	lastInsertId uint64
}

func NewResult(affectedRows, lastInsertId uint64) *Result {
	return &Result{affectedRows, lastInsertId}
}

func (r *Result) AffectedRows() uint64 {
	return r.affectedRows
}

func (r *Result) LastInsertId() uint64 {
	return r.lastInsertId
}
