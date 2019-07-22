// traversalsource.go

package deep6

import "context"

func traversalSource(ctx context.Context, id string) (
	<-chan TraversalData, // pass data on to next stage
	<-chan error, // emits errors encountered to the pipeline
	error) {

	out := make(chan TraversalData)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		td := TraversalData{TraversalMatches: make(map[string]string, 0), TraversalStageTargets: make(map[string]string, 0)}
		td.TraversalStageTargets[id] = "unknown"

		select {
		case out <- td: // pass the data on to the next stage
		case <-ctx.Done(): // listen for pipeline shutdown
			return
		}

	}()

	return out, errc, nil

}
