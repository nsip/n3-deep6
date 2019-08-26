// jsoniterator.go

package deep6

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
)

//
// Iterator for json objects presented through a reader; file,
// http request, stdin etc.
//
// Reads json data from an array in a stream such as a file
// and feeds each parsed object into the ingest pipeline.
//
// ctx - required context for pipeline management
// c - channel providing json objects as []bytes
//
func jsonIteratorSource(ctx context.Context, c <-chan []byte) (
	<-chan map[string]interface{}, // source emits json objects read from file as map
	<-chan error, // emits any errors encountered to the pipeline
	error) { // any error when creating the source stage itself

	out := make(chan map[string]interface{})
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		for jsonBytes := range c {
			var m map[string]interface{}
			if err := json.Unmarshal(jsonBytes, &m); err != nil {
				errc <- errors.Wrap(err, "unable to unmarshal json jsonIteratorSource():")
				return
			}

			select {
			case out <- m: // pass the map onto the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}
		}

	}()

	return out, errc, nil
}
