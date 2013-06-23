package db

// The objective of this structure is to maintain state, while crawling the associations and building the entity tree, after an sql select
// This way it is possible to hint the row transformers wich paths to follow.
// While crawling the associations of a query result the list of possible paths changes.

// The process has 2 steps:
// 1) building a tree (common foreign keys are converted into one) 
//    to identify wich branches exist for a node
// 2) building an array that only has the edge nodes of a tree (tree contour) 
//    to ease the computation of the column offsets of each processed entity
type Crawler struct {
	depth     int
	nodes     []*CrawlerNode
	firstNode *CrawlerNode
}

func (this *Crawler) GetBranches() []*CrawlerNode {
	if this.nodes == nil || this.depth >= len(this.nodes) {
		return nil
	}

	return this.nodes[this.depth].GetBranches()
}

func (this *Crawler) Dispose() {
	this.depth = 0
	this.nodes = nil
	this.firstNode = nil
}

func (this *Crawler) Prepare(query *Query) {
	table := query.GetTable()
	var includes [][]*Association
	for _, join := range query.GetJoins() {
		includes = append(includes, join.GetAssociations())
	}

	// reset
	this.depth = 0
	this.firstNode = new(CrawlerNode)
	// holds the last table
	holder := &HoldTable{table}
	for _, fks := range includes {
		this.firstNode.BuildTree(fks, holder)
	}

	this.nodes = this.firstNode.FlatenTree(make([]*CrawlerNode, 0))
}

func (this *Crawler) Forward() {
	this.depth++
}

func (this *Crawler) Rewind() {
	this.depth = 0
}
