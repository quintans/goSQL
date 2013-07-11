package db

import (
	tk "github.com/quintans/toolkit"
	"strings"
)

/*
Holds a information about a virtual column.
A virtual column is a column declared in a table, but exists in a adjacent table.
This is useful for columns that are internationalized. ex: BOOK [1]--[*] BOOK_I18N
*/
type VirtualColumn struct {
	Association *Association // navigation to the table holding the REAL column
	Column      *Column
}

func newVirtualColumn(column *Column, association *Association) *VirtualColumn {
	vc := new(VirtualColumn)
	vc.Association = association
	vc.Column = column
	return vc
}

type Column struct {
	table     *Table // the table that this column belongs
	name      string // column name
	alias     string // column alias
	key       bool
	mandatory bool
	version   bool
	deletion  bool
	virtual   *VirtualColumn
	hash      int
}

// Param alias: The alias of the column
// return
func (this *Column) As(alias string) *Column {
	this.alias = alias
	return this
}

// DEfines the table alias for this column in the SQL
func (this *Column) For(tableAlias string) *ColumnHolder {
	return NewColumnHolder(this).For(tableAlias)
}

// set this as a key column
//
// return
func (this *Column) Key() *Column {
	this.key = true
	this.table.addKey(this)
	return this
}

// set this as a mandatory column
//
// return
func (this *Column) Mandatory() *Column {
	this.mandatory = true
	return this
}

//	/**
//	 * set this as a version column
//	 *
//	 * @return
//	 */
func (this *Column) Version() *Column {
	this.version = true
	this.table.setVersion(this)
	return this
}

//	/**
//	 * set this as a deletion column
//	 *
//	 * @return
//	 */
func (this *Column) Deletion() *Column {
	this.deletion = true
	this.table.setDeletion(this)
	return this
}

//	Gets the table that this column belongs to
//
//	returns the table
func (this *Column) GetTable() *Table {
	return this.table
}

func (this *Column) GetAlias() string {
	return this.alias
}

//	/**
//	 * obtem o nome da coluna
//	 *
//	 * @return nome da coluna
//	 */
func (this *Column) GetName() string {
	return this.name
}

//	/**
//	 * indica se é uma coluna chave
//	 *
//	 * @return se é coluna chave
//	 */
func (this *Column) IsKey() bool {
	return this.key
}

func (this *Column) IsMandatory() bool {
	return this.mandatory
}

func (this *Column) IsVersion() bool {
	return this.version
}

func (this *Column) IsDeletion() bool {
	return this.deletion
}

//	/**
//	 * devolve a representação em String desta coluna.
//	 *
//	 * @return devolve string com o formato 'table.coluna'
//	 */
func (this *Column) String() string {
	return this.table.String() + "." + this.name
}

func (this *Column) Equals(o interface{}) bool {
	switch t := o.(type) { //type switch
	case *Column:
		return (t.table.Equals(this.table) &&
			strings.ToUpper(this.name) == strings.ToUpper(t.name))
	}
	return false
}

func (this *Column) HashCode() int {
	if this.hash == 0 {
		result := tk.HashType(tk.HASH_SEED, this)
		result = tk.HashString(result, this.table.String()+"."+this.name)
		this.hash = result
	}

	return this.hash
}

func (this *Column) Clone() interface{} {
	panic("Clone for Column is not implemented")
}

func (this *Column) IsVirtual() bool {
	return this.virtual != nil
}

func (this *Column) GetVirtual() *VirtualColumn {
	return this.virtual
}

// CONDITION ===========================
func (this *Column) Greater(value interface{}) *Criteria {
	return Greater(this, value)
}

func (this *Column) GreaterOrMatch(value interface{}) *Criteria {
	return GreaterOrMatch(this, value)
}

func (this *Column) Lesser(value interface{}) *Criteria {
	return Lesser(this, value)
}

func (this *Column) LesserOrMatch(value interface{}) *Criteria {
	return LesserOrMatch(this, value)
}

func (this *Column) Matches(value interface{}) *Criteria {
	return Matches(this, value)
}

func (this *Column) IMatches(value interface{}) *Criteria {
	return IMatches(this, value)
}

func (this *Column) Like(right interface{}) *Criteria {
	return Like(this, right)
}

func (this *Column) ILike(right interface{}) *Criteria {
	return ILike(this, right)
}

func (this *Column) Different(value interface{}) *Criteria {
	return Different(this, value)
}

func (this *Column) IsNull() *Criteria {
	return IsNull(NewColumnHolder(this))
}

func (this *Column) In(value ...interface{}) *Criteria {
	return In(this, value...)
}

func (this *Column) Range(left, right interface{}) *Criteria {
	return Range(this, left, right)
}
