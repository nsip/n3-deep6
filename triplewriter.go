// triplewriter.go

package deep6

import (
	"context"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
)

//
// commits triples to datastore so can be used in
// lookups by later pipeline stages.
//
// ctx - context for pipeline management
// wb - badger.WriteBatch which manages very fast writing to the
// datastore
// in - channel providing IngestData objects
//
func tripleWriter(ctx context.Context, wb *badger.WriteBatch, in <-chan IngestData) (
	<-chan IngestData, // pass on to next stage
	<-chan error, // emits errors encountered to the pipeline
	error) { // returns any error encountered creating this component

	out := make(chan IngestData)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		for igd := range in {
			for _, t := range igd.Triples {
				for _, hexa := range t.Sextuple() { // turn each tuple into hexastore entries
					err := wb.Set([]byte(hexa), []byte{})
					if err != nil {
						errc <- errors.Wrap(err, "error writing triple to datastore:")
						return
					}
				}
			}
			select {
			case out <- igd: // pass the data on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}
		}
	}()

	return out, errc, nil

}
