package db

type Union struct {
	Query *Query
	All   bool
}

func (u *Union) Equals(o interface{}) bool {
	return u == o
}
