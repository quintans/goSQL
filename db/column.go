package db

import (
	"strings"

	tk "github.com/quintans/toolkit"
)

type Column struct {
	table     *Table // the table that this column belongs
	name      string // column name
	alias     string // column alias
	key       bool
	mandatory bool
	version   bool
	deletion  bool
	hash      int

	err error
}

// Param alias: The alias of the column
// return
func (c *Column) As(alias string) *Column {
	c.alias = alias
	return c
}

// Defines the table alias for this column in the SQL
func (c *Column) For(tableAlias string) *ColumnHolder {
	return NewColumnHolder(c).For(tableAlias)
}

// set this as a key column
//
// return
func (c *Column) Key() *Column {
	c.key = true
	c.table.addKey(c)
	return c
}

// set this as a mandatory column
//
// return
func (c *Column) Mandatory() *Column {
	c.mandatory = true
	return c
}

//	/**
//	 * set this as a version column
//	 *
//	 * @return
//	 */
func (c *Column) Version() *Column {
	c.version = true
	c.table.setVersion(c)
	return c
}

//	/**
//	 * set this as a deletion column
//	 *
//	 * @return
//	 */
func (c *Column) Deletion() *Column {
	c.deletion = true
	c.table.setDeletion(c)
	return c
}

//	Gets the table that this column belongs to
//
//	returns the table
func (c *Column) GetTable() *Table {
	return c.table
}

func (c *Column) GetAlias() string {
	return c.alias
}

//	/**
//	 * obtem o nome da coluna
//	 *
//	 * @return nome da coluna
//	 */
func (c *Column) GetName() string {
	return c.name
}

//	/**
//	 * indica se é uma coluna chave
//	 *
//	 * @return se é coluna chave
//	 */
func (c *Column) IsKey() bool {
	return c.key
}

func (c *Column) IsMandatory() bool {
	return c.mandatory
}

func (c *Column) IsVersion() bool {
	return c.version
}

func (c *Column) IsDeletion() bool {
	return c.deletion
}

//	/**
//	 * devolve a representação em String desta coluna.
//	 *
//	 * @return devolve string com o formato 'table.coluna'
//	 */
func (c *Column) String() string {
	return c.table.String() + "." + c.name
}

func (c *Column) Equals(o interface{}) bool {
	switch t := o.(type) { //type switch
	case *Column:
		return (t.table.Equals(c.table) &&
			strings.EqualFold(c.name, t.name))
	}
	return false
}

func (c *Column) HashCode() int {
	if c.hash == 0 {
		result := tk.HashType(tk.HASH_SEED, c)
		result = tk.HashString(result, c.table.String()+"."+c.name)
		c.hash = result
	}

	return c.hash
}

func (c *Column) Clone() interface{} {
	panic("Clone for Column is not implemented")
}

// CONDITION ===========================

func (c *Column) Greater(value interface{}) *Criteria {
	return Greater(c, value)
}

func (c *Column) GreaterOrMatch(value interface{}) *Criteria {
	return GreaterOrMatch(c, value)
}

func (c *Column) Lesser(value interface{}) *Criteria {
	return Lesser(c, value)
}

func (c *Column) LesserOrMatch(value interface{}) *Criteria {
	return LesserOrMatch(c, value)
}

func (c *Column) Matches(value interface{}) *Criteria {
	return Matches(c, value)
}

func (c *Column) IMatches(value interface{}) *Criteria {
	return IMatches(c, value)
}

func (c *Column) Like(right interface{}) *Criteria {
	return Like(c, right)
}

func (c *Column) ILike(right interface{}) *Criteria {
	return ILike(c, right)
}

func (c *Column) Different(value interface{}) *Criteria {
	return Different(c, value)
}

func (c *Column) IsNull() *Criteria {
	return IsNull(NewColumnHolder(c))
}

func (c *Column) In(value ...interface{}) *Criteria {
	return In(c, value...)
}

func (c *Column) Range(left, right interface{}) *Criteria {
	return Range(c, left, right)
}
