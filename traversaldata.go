// traversaldata.go

package deep6

//
// Data strucure to move
// between stages of a graph traversal
// pipeline
//
type TraversalData struct {
	//
	// Triples that successfully meet the
	// traversal spec requirements
	//
	// this data is persisted between stages of the
	// pipeline
	//
	// Stores the id and type of the match
	//
	TraversalMatches map[string]string
	//
	// Triples to be considered by next stage
	//
	// each stage replaces this data with new targets
	//
	TraversalStageTargets map[string]string
}
