package unit_tests

import (
	"dist-mq/delivery"
	"dist-mq/node"
)

// delivery declares Cluster rather than importing node, so nothing in either
// package catches drift between them. This does, in the one place that imports
// both.
var _ delivery.Cluster = (*node.Node)(nil)
