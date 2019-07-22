// linkwriter.go

package deep6

import (
	"context"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"
)

//
// commits all inter-object graph links to the datastore
//
// ctx - context for pipeline management
// wb - badger.WriteBatch for fast writes to db
// in - channel providing IngestData objects
//
func linkWriter(ctx context.Context, wb *badger.WriteBatch, in <-chan IngestData) (
	<-chan IngestData, // new list of triples also containing links
	<-chan error, // emits errors encountered to the pipeline
	error) { // returns any error encountered constructing this component

	out := make(chan IngestData)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		for igd := range in {
			for _, t := range igd.LinkTriples {
				for _, hexa := range t.SextupleLink() {
					err := wb.Set([]byte(hexa), []byte{})
					if err != nil {
						errc <- errors.Wrap(err, "error writing link triples: ")
						return
					}
				}
			}
			select {
			case out <- igd: // pass the map onto the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}
		}
	}()

	return out, errc, nil

}
