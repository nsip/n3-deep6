// traversalhydrator.go

package deep6

import (
	"context"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
)

//
// Traversal queries work by assembling lists of relevant objects based
// on links.
//
// This component re-inflates whole objects from the ids
// and stores the whole objects in the provided map.
//
func traversalHydrator(ctx context.Context, resultsReceiver *map[string][]map[string]interface{},
	db *badger.DB, in <-chan TraversalData) (
	<-chan TraversalData, // pass data on to next stage
	<-chan error, // emits errors encountered to the pipeline
	error) {

	out := make(chan TraversalData)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		for td := range in {

			resultsByType := make(map[string][]map[string]interface{})
			for match, objectType := range td.TraversalMatches {
				result, err := findById(match, db)
				if err != nil {
					errc <- errors.Wrap(err, "traversal-hydrator cannot find target object: "+match)
				}
				//
				// remove n3 object properties
				//
				delete(result, "is-a")
				delete(result, "unique")
				if len(result) > 0 { // property/unique.links will be empty after tidy-up above()
					resultsByType[objectType] = append(resultsByType[objectType], result)
				}
			}
			*resultsReceiver = resultsByType

			select {
			case out <- td: // pass the data on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}
		}
	}()

	return out, errc, nil

}
