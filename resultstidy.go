// resultstidy.go

package deep6

import (
	"context"
	"errors"
)

//
// resultsTidy removes n3 specific properties from objects
// has the side effect of pruning PropertyLink objects
// from the results stream, where they add no value.
// Also collates objects by type in the results receiver.
//
func resultsTidy(ctx context.Context, resultsReceiver *map[string][]map[string]interface{}, in <-chan map[string]interface{}) (
	<-chan map[string]interface{},
	<-chan error, // emits errors encountered to the pipeline
	error) {
	out := make(chan map[string]interface{})
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		for m := range in {

			objectType, ok := m["is-a"].(string)
			if !ok {
				errc <- errors.New("object with no type encountered in objectTidy():")
				return
			}

			//
			// remove n3 object properties
			//
			delete(m, "is-a")
			delete(m, "unique")
			if len(m) == 0 { // property.links/unique.links will be empty after tidy-up
				continue
			}

			//
			// store the object by type in the results collection
			//
			(*resultsReceiver)[objectType] = append((*resultsReceiver)[objectType], m)

			select {
			case out <- m: // pass the data on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}
		}

	}()

	return out, errc, nil
}
