// objectRemover.go

package deep6

import (
	"context"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"
	boom "github.com/tylertreat/BoomFilters"
)

//
// checks to see if this is an update to an existing object in the graph
// if so removes the current version to make way for new one.
//
// ctx: Context
// db: the badger instance
// wb: WriteBatch from the db to handle deletes
// sbf: boom filter for classifier
// auditLevel: diagnostic ouput level
// folderPath: support file location for configs etc.
// in: inbound channel of ingest data strucures
//
func objectRemover(ctx context.Context, db *badger.DB, wb *badger.WriteBatch, sbf *boom.ScalableBloomFilter, auditLevel, folderPath string, in <-chan IngestData) (
	<-chan IngestData, // pass on to next stage
	<-chan error, // emits errors encountered to the pipeline
	error) { // returns any error encountered creating this component

	out := make(chan IngestData)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		for igd := range in {
			id := igd.N3id
			err := deleteWithID(id, db, wb, sbf, auditLevel, folderPath)
			if err != nil && err != ErrNotFound {
				errc <- errors.Wrap(err, "error removing existing object")
				return
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
