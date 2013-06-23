package db

type Union struct {
	Query *Query
	All   bool
}

func (this *Union) Equals(o interface{}) bool {
	if this == o {
		return true
	}

	return false
}
