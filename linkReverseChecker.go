// linkbuilder.go

package deep6

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"
)

//
// Given a set of unfulfiled candidate links for an object, checks the hexastore to
// find matches that need linking to.
// Used for matching data that arrived historically, and did not register
// a feature of interest, e.g. from a different data-model; SIF will link
// internally backwards & forwards, but if xapi data arrives after SIF data
// this back-check is required as xapi will not be looking for
// refids or sif local-ids, but will be looking for particular
// values (.Object) properties.
//
// ctx - pipeline management context
// db - badger db used for lookups of objects to link to
// in - channel providing IngestData objects
//
func linkReverseChecker(ctx context.Context, db *badger.DB, in <-chan IngestData) (
	<-chan IngestData, // pass data on to next stage
	<-chan error, // emits errors encountered to the pipeline
	error) {

	out := make(chan IngestData)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		for igd := range in {
			linksTo := make(map[string]interface{}, 0)
			// first see if anything reverse links
			// by checking for the presence of the object member
			err := db.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()
				for _, candidate := range igd.LinkCandidates {
					prefix := []byte(fmt.Sprintf("ops|%s|", candidate.O))
					for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
						item := it.Item()
						t := NewTriple(string(item.KeyCopy(nil)))
						linksTo[t.S] = struct{}{}
					}
				}
				return nil
			})
			if err != nil {
				errc <- errors.Wrap(err, "linkReverseChecker() database iterator error:")
				return
			}

			// add any reverse links to the list of viable candidates
			for k, _ := range linksTo {
				reverseLinkTriple := Triple{
					S: "reverse",
					P: "link",
					O: k,
				}
				igd.LinkCandidates = append(igd.LinkCandidates, reverseLinkTriple)
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
