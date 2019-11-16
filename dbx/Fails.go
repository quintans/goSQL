package dbx

import tk "github.com/quintans/toolkit"

const FAULT_PREP_STATEMENT = "STMT01"
const FAULT_EXEC_STATEMENT = "STMT02"
const FAULT_PARSE_STATEMENT = "STMT03"
const FAULT_VALUES_STATEMENT = "STMT04"
const FAULT_QUERY = "QRY01"
const FAULT_TRANSFORM = "TRF01"
const FAULT_OPTIMISTIC_LOCK = "OPT_LOCK"

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
