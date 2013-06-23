package db

type HoldTable struct {
	what *Table
}

type CrawlerNode struct {
	ForeignKey *Association
	branches   []*CrawlerNode
}

func (this *CrawlerNode) String() string {
	if this.ForeignKey != nil {
		return this.ForeignKey.Alias
	}

	return "."
}

func (this *CrawlerNode) GetBranches() []*CrawlerNode {
	return this.branches
}

// To guarantee that the several joins are chained and assessed correctly
// a tree is built only for validation
func (this *CrawlerNode) BuildTree(fks []*Association, table *HoldTable) {
	if this.branches == nil {
		this.branches = make([]*CrawlerNode, 0)
	}

	var found *CrawlerNode
	for _, node := range this.branches {
		if fks[0].Path() == node.ForeignKey.Path() {
			found = node
			break
		}
	}

	if found == nil {
		// new branche
		found = new(CrawlerNode)
		found.ForeignKey = fks[0]
		this.branches = append(this.branches, found)
		table.what = fks[0].GetTableTo()
	}

	if len(fks) > 1 {
		found.BuildTree(this.dropFirst(fks), table)
	}
}

// builds an array with all the tree nodes by entry order 
// param: flat
func (this *CrawlerNode) FlatenTree(flat []*CrawlerNode) []*CrawlerNode {
	nodes := append(flat, this)
	if this.branches != nil {
		for _, node := range this.branches {
			nodes = node.FlatenTree(nodes)
		}
	}
	return nodes
}

func (this *CrawlerNode) dropFirst(fks []*Association) []*Association {
	length := len(fks)
	if length == 0 {
		return fks
	} else if length == 1 {
		return make([]*Association, 0)
	}

	split := make([]*Association, length-1)
	for i := 0; i < len(split); i++ {
		split[i] = fks[i+1]
	}
	return split
}
