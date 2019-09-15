// linkbuilder.go

package deep6

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"
)

//
// Given a set of candidate links for an object, checks the hexastore to
// find matches that need linking to.
//
// ctx - pipeline management context
// db - badger db used for lookups of objects to link to
// wb - badger.Writebatch for fast writing of new link objects
// in - channel providing IngestData objects
//
func linkBuilder(ctx context.Context, db *badger.DB, wb *badger.WriteBatch, in <-chan IngestData) (
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
			// first see if anything links
			err := db.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()
				for _, candidate := range igd.LinkCandidates {
					prefix := []byte(fmt.Sprintf("spo|%s|is-a|", candidate.O))
					for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
						item := it.Item()
						t := NewTriple(string(item.KeyCopy(nil)))
						linksTo[t.S] = struct{}{}
					}
				}
				return nil
			})
			if err != nil {
				errc <- errors.Wrap(err, "linkbuilder database iterator error:")
				return
			}

			//
			// if not all specified links are satisfied, then a new link
			// entity needs to be created
			//
			if len(linksTo) < len(igd.LinkCandidates) {
				for _, candidate := range igd.LinkCandidates {
					if _, ok := linksTo[candidate.O]; ok { // only build a link if needed
						continue
					}
					if candidate.O == "" { // ignore empty links
						continue
					}
					if candidate.O == igd.Unique { // ignore the pseudo-unique link here
						continue
					}
					// if needed generate new triple and store in the db
					propertyLinkTriple := Triple{
						S: candidate.O,
						P: "is-a",
						O: "Property.Link",
					}
					for _, t := range propertyLinkTriple.Sextuple() {
						err := wb.Set([]byte(t), []byte{})
						if err != nil {
							errc <- errors.Wrap(err, "cannot commit propertylink:")
							return
						}
						// add new link
						linksTo[propertyLinkTriple.S] = struct{}{}
					}
				}
			}
			//
			// now create links to any declared psuedo-unique properties;
			// forces a property link to exist that creates a unique identity
			// for the object if its own data has no available discrimination
			//
			if len(igd.Unique) > 0 {
				uniqueLinkTriple := Triple{
					S: igd.Unique,
					P: "is-a",
					O: "Unique.Link",
				}
				for _, t := range uniqueLinkTriple.Sextuple() {
					err := wb.Set([]byte(t), []byte{})
					if err != nil {
						errc <- errors.Wrap(err, "cannot commit uniquelink:")
						return
					}
					linksTo[uniqueLinkTriple.S] = struct{}{}
				}
			}

			// convert all known links into link triples
			linkTriples := make([]Triple, 0)
			for l, _ := range linksTo {
				if l == igd.N3id {
					continue // don't self link
				}
				t := Triple{
					S: igd.N3id,
					P: "references",
					O: l,
				}
				linkTriples = append(linkTriples, t)
			}
			igd.LinkTriples = linkTriples

			select {
			case out <- igd: // pass the data on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}
		}
	}()

	return out, errc, nil

}
