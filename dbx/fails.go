package dbx

import tk "github.com/quintans/toolkit"

const FAULT_PARSE_STATEMENT = "sql-parse"
const FAULT_VALUES_STATEMENT = "sql-values"
const FAULT_OPTIMISTIC_LOCK = "optimistic-lock"

var _ error = (*PersistenceFail)(nil)

type PersistenceFail struct {
	*tk.Fail
}

func NewPersistenceFail(code string, message string) *PersistenceFail {
	fail := new(PersistenceFail)
	fail.Fail = new(tk.Fail)
	fail.Fail.Code = code
	fail.Fail.Message = message
	return fail
}

var _ error = &OptimisticLockFail{}

type OptimisticLockFail struct {
	*tk.Fail
}

func NewOptimisticLockFail(message string) *OptimisticLockFail {
	fail := new(OptimisticLockFail)
	fail.Fail = new(tk.Fail)
	fail.Fail.Code = FAULT_OPTIMISTIC_LOCK
	fail.Fail.Message = message
	return fail
}
