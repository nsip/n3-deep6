// traverselinks.go

package deep6

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
)

func traverseLinks(ctx context.Context, db *badger.DB, in <-chan TraversalData) (
	<-chan TraversalData, // pass data on to next stage
	<-chan error, // emits errors encountered to the pipeline
	error) {

	out := make(chan TraversalData)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		for td := range in {
			targets := make(map[string]string)
			err := db.View(func(txn *badger.Txn) error {
				for id, targetType := range td.TraversalStageTargets {
					opts := badger.DefaultIteratorOptions
					opts.PrefetchValues = false
					it := txn.NewIterator(opts)
					defer it.Close()
					linkinPrefix := []byte(fmt.Sprintf("posl|references|%s|", id)) // things that link to this object
					for it.Seek(linkinPrefix); it.ValidForPrefix(linkinPrefix); it.Next() {
						item := it.Item()
						t := NewTriple(string(item.KeyCopy(nil)))
						targets[t.S] = targetType
					}
					linkoutPrefix := []byte(fmt.Sprintf("psol|references|%s|", id)) // objects we link to
					for it.Seek(linkoutPrefix); it.ValidForPrefix(linkoutPrefix); it.Next() {
						item := it.Item()
						t := NewTriple(string(item.KeyCopy(nil)))
						targets[t.O] = targetType
					}

				}
				return nil
			})
			if err != nil {
				errc <- errors.Wrap(err, "traverselinks database iterator error:")
				return
			}

			td.TraversalStageTargets = targets

			select {
			case out <- td: // pass the data on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}
		}
	}()

	return out, errc, nil

}
