package dbx

import tk "github.com/quintans/toolkit"

const FAULT_PREP_STATEMENT = "STMT01"
const FAULT_EXEC_STATEMENT = "STMT02"
const FAULT_PARSE_STATEMENT = "STMT03"
const FAULT_VALUES_STATEMENT = "STMT04"
const FAULT_QUERY = "QRY01"
const FAULT_TRANSFORM = "TRF01"
const FAULT_OPTIMISTIC_LOCK = "OPT_LOCK"

type PersistenceFail struct {
	tk.Fail
}

func NewPersistenceFail(code string, message string) *PersistenceFail {
	fail := new(PersistenceFail)
	fail.Code = code
	fail.Message = message
	return fail
}

type OptimisticLockFail struct {
	tk.Fail
}

func NewOptimisticLockFail(code string, message string) *OptimisticLockFail {
	fail := new(OptimisticLockFail)
	fail.Code = code
	fail.Message = message
	return fail
}
