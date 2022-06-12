package db

/*
The objective of this structure is to maintain state, while crawling the associations and building the entity tree, after an sql select
This way it is possible to hint the row transformers wich paths to follow.
While crawling the associations of a query result the list of possible paths changes.

The process has 2 steps:
1) building a tree (common foreign keys are converted into one)
   to identify wich branches exist for a node
2) building an array that only has the edge nodes of a tree (tree contour)
   to ease the computation of the column offsets of each processed entity
*/
type Crawler struct {
	depth     int
	nodes     []*CrawlerNode
	firstNode *CrawlerNode
}

func (c *Crawler) GetBranches() []*CrawlerNode {
	if c.nodes == nil || c.depth >= len(c.nodes) {
		return nil
	}

	return c.nodes[c.depth].GetBranches()
}

func (c *Crawler) Dispose() {
	c.depth = 0
	c.nodes = nil
	c.firstNode = nil
}

func (c *Crawler) Prepare(query *Query) {
	table := query.GetTable()
	var includes [][]*Association
	for _, join := range query.GetJoins() {
		if join.IsFetch() {
			includes = append(includes, join.GetAssociations())
		}
	}

	// reset
	c.depth = 0
	c.firstNode = new(CrawlerNode)
	// holds the last table
	holder := &HoldTable{table}
	for _, fks := range includes {
		c.firstNode.BuildTree(fks, holder)
	}

	c.nodes = c.firstNode.FlatenTree(make([]*CrawlerNode, 0))
}

func (c *Crawler) Forward() {
	c.depth++
}

func (c *Crawler) Rewind() {
	c.depth = 0
}
