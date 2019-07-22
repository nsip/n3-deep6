// resultssource.go

package deep6

import (
	"context"
)

//
// utiltiy componnet used to feed a results set into a pipelene
// suc as filter()
//
func resultsSource(ctx context.Context, results []map[string]interface{}) (
	<-chan map[string]interface{}, // source emits individual objects from the results set
	<-chan error, // emits any errors encountered to the pipeline
	error) { // any error when creating the source stage itself

	out := make(chan map[string]interface{})
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		// read objects one by one
		for _, result := range results {
			select {
			case out <- result: // pass the map onto the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}

		}
	}()

	return out, errc, nil

}
