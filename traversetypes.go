// traversaltype.go

package deep6

import (
	"context"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
)

func traverseTypes(ctx context.Context, objectType string, filterSpec FilterSpec, db *badger.DB, in <-chan TraversalData) (
	<-chan TraversalData, // pass data on to next stage
	<-chan error, // emits errors encountered to the pipeline
	error) {

	out := make(chan TraversalData)
	errc := make(chan error, 1)

	if objectType == "" {
		return nil, nil, errors.New("Cannot have empty traversal-spec member")
	}

	go func() {
		defer close(out)
		defer close(errc)

		for td := range in {

			matches := make(map[string]string)
			err := db.View(func(txn *badger.Txn) error {
				for id := range td.TraversalStageTargets {
					opts := badger.DefaultIteratorOptions
					opts.PrefetchValues = false
					it := txn.NewIterator(opts)
					defer it.Close()
					prefix := []byte(fmt.Sprintf("osp|%s|%s|is-a", objectType, id))
					for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
						item := it.Item()
						t := NewTriple(string(item.KeyCopy(nil)))
						matches[t.S] = objectType
					}
				}
				return nil
			})
			if err != nil {
				errc <- errors.Wrap(err, "traversebytype database iterator error:")
				return
			}

			//
			// apply filters for objects of this type
			//
			filters, ok := filterSpec[objectType]
			if ok {
				filteredIds := make(map[string]string)
				for match := range matches {
					object, err := findById(match, db)
					if err != nil {
						errc <- errors.Wrap(err, "TraverseTypes: could not retrieve object for filtering: ")
						return
					}
					m := Flatten(object)
					for k, v := range m {
						filtersPassed := 0
						for _, filter := range filters {
							if strings.Contains(k, filter.Predicate) && v == filter.TargetValue {
								filtersPassed++
							}
						}
						if len(filters) == filtersPassed {
							filteredIds[match] = objectType

						}
					}
				}
				for k, v := range filteredIds {
					_, ok := td.TraversalMatches[k] // don't overwrite an object reference with a weaker link
					if ok {
						if (v == "Property.Link") || (v == "Unique.Link") {
							continue
						}
					}
					td.TraversalMatches[k] = v
				}
				td.TraversalStageTargets = filteredIds
			} else {
				for k, v := range matches {
					_, ok := td.TraversalMatches[k] // don't overwrite an object reference with a weaker link
					if ok {
						if (v == "Property.Link") || (v == "Unique.Link") {
							continue
						}
					}
					td.TraversalMatches[k] = v
				}
				td.TraversalStageTargets = matches
			}

			select {
			case out <- td: // pass the data on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}
		}
	}()

	return out, errc, nil

}
