package db

type HoldTable struct {
	what *Table
}

type CrawlerNode struct {
	ForeignKey *Association
	branches   []*CrawlerNode
}

func (c *CrawlerNode) String() string {
	if c.ForeignKey != nil {
		return c.ForeignKey.Alias
	}

	return "."
}

func (c *CrawlerNode) GetBranches() []*CrawlerNode {
	return c.branches
}

// To guarantee that the several joins are chained and assessed correctly
// a tree is built only for validation
func (c *CrawlerNode) BuildTree(fks []*Association, table *HoldTable) {
	if c.branches == nil {
		c.branches = make([]*CrawlerNode, 0)
	}

	var found *CrawlerNode
	for _, node := range c.branches {
		if fks[0].Path() == node.ForeignKey.Path() {
			found = node
			break
		}
	}

	if found == nil {
		// new branche
		found = new(CrawlerNode)
		found.ForeignKey = fks[0]
		c.branches = append(c.branches, found)
		table.what = fks[0].GetTableTo()
	}

	if len(fks) > 1 {
		found.BuildTree(c.dropFirst(fks), table)
	}
}

// builds an array with all the tree nodes by entry order
// param: flat
func (c *CrawlerNode) FlatenTree(flat []*CrawlerNode) []*CrawlerNode {
	nodes := append(flat, c)
	if c.branches != nil {
		for _, node := range c.branches {
			nodes = node.FlatenTree(nodes)
		}
	}
	return nodes
}

func (c *CrawlerNode) dropFirst(fks []*Association) []*Association {
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
