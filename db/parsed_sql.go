package db

// Holds information about a parsed SQL statement.
type ParsedSql struct {
	sql   string
	Names []string
	// indexes for the specified parameter.
	Indexes [][]int
	// total count of all of the parameters in the SQL statement.
}

// param originalSql: the SQL statement that is being (or is to be) parsed
func NewParsedSql(sql string) *ParsedSql {
	this := &ParsedSql{sql: sql}
	this.Names = make([]string, 0)
	this.Indexes = make([][]int, 0)
	return this
}

// Add a named parameter parsed from this SQL statement.
//
// param name: the name of the parameter
// param startIndex: the start index in the original SQL String
// param endIndex: the end index in the original SQL String
func (p *ParsedSql) AddNamedParameter(name string, startIndex int, endIndex int) {
	p.Names = append(p.Names, name)
	p.Indexes = append(p.Indexes, []int{startIndex, endIndex})
}

func (p ParsedSql) String() string {
	return p.sql
}
