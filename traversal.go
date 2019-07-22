// traversal.go

package deep6

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"
)

//
// Data type for traversal specification
//
// Each element is be an object type name
// the traversal finds objects of each type
// in the order specified as long as there is some
// forward or backward link available.
//
type Traversal struct {
	TraversalSpec []string
}

//
// Follows a traversal spec starting with an object
// found to have the supplied initial value
//
//
//  id - the value to search for
//  traversalspec - object types to traverse
//
func (d6 *Deep6DB) TraversalWithId(id string, t Traversal, filterspec FilterSpec) (map[string][]map[string]interface{}, error) {

	defer timeTrack(time.Now(), "TraversalWithId()")

	results, err := traversalWithId(id, t.TraversalSpec, filterspec, d6.db, d6.AuditLevel)
	if err != nil {
		return nil, err
	}

	return results, nil

}

func traversalWithId(id string, traversalspec []string, filterspec FilterSpec, db *badger.DB, auditLevel string) (map[string][]map[string]interface{}, error) {

	if len(traversalspec) == 0 {
		return nil, errors.New("no traversalspec provided")
	}

	results := make(map[string][]map[string]interface{}, 0)

	// set up a context to manage traversal pipeline
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// monitor all error channels
	var errcList []<-chan error

	//
	// feed the pipeline with a prepared data structure
	//
	sourceOut, errc, err := traversalSource(ctx, id)
	if err != nil {
		return nil, errors.Wrap(err, "Error: cannot create traversal source: ")
	}
	errcList = append(errcList, errc)

	//
	// check that the object id matches the first term of the traversal
	//
	typeOut, errc, err := traverseTypes(ctx, traversalspec[0], filterspec, db, sourceOut)
	if err != nil {
		return nil, errors.Wrap(err, "Error: cannot create traversal-by-type component: ")
	}
	errcList = append(errcList, errc)

	//
	// for the rest of the traversal follow the following patter:
	//
	// find links from the current object (in and out)
	// filter the linked objects by the next object-type in the traversal
	//
	// a pair of links->type match components is put in place for each
	// term in the traversal spec
	//
	head := typeOut
	var next_chan <-chan TraversalData
	for _, specObject := range traversalspec[1:] {
		if next_chan == nil {
			link_chan, errc, err := traverseLinks(ctx, db, head)
			if err != nil {
				errors.Wrap(err, "Error: cannot create traversal-by-links component: ")
			}
			errcList = append(errcList, errc)
			next_chan, errc, err = traverseTypes(ctx, specObject, filterspec, db, link_chan)
			if err != nil {
				errors.Wrap(err, "Error: cannot create traversal-by-type component: ")
			}
			errcList = append(errcList, errc)
		} else {
			link_chan, errc, err := traverseLinks(ctx, db, next_chan)
			if err != nil {
				errors.Wrap(err, "Error: cannot create traversal-by-links component: ")
			}
			errcList = append(errcList, errc)
			next_chan, errc, err = traverseTypes(ctx, specObject, filterspec, db, link_chan)
			if err != nil {
				errors.Wrap(err, "Error: cannot create traversal-by-type component: ")
			}
			errcList = append(errcList, errc)
		}
	}

	//
	// Graph traversal above is all about links, when we get here
	// we take the found object ids and
	// turn them back into full json objects
	//
	hydratorOut, errc, err := traversalHydrator(ctx, &results, db, next_chan)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create traversal hydrator: ")
	}
	errcList = append(errcList, errc)

	//
	// audit sink monitors links and hits found by the traversal
	// and writes them out to console/file
	//
	//
	errc, err = traversalAuditSink(ctx, auditLevel, hydratorOut)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create traversal-audit: ")
	}
	errcList = append(errcList, errc)

	// monitor progress
	err = WaitForPipeline(errcList...)

	return results, err

}

//
// Follows a traversal spec starting with an object
// found to have the supplied value
//
// Unlike find by id this traversal may find multiple matches
// so dataset returned can contain mmultiple result arrays,
// but data will be constrained by the Types of the traversal-spec.
//
// Allows traversal using for example a localid for a user, or a user name
// as opposed to a unique id
//
//  val - the value to search for
//  traversalspec - object types to traverse
//
func (d6 *Deep6DB) TraversalWithValue(val string, t Traversal, filterspec FilterSpec) (map[string][]map[string]interface{}, error) {

	defer timeTrack(time.Now(), "TraversalWithValue()")

	return traversalWithValue(val, t.TraversalSpec, filterspec, d6.db, d6.AuditLevel)

}

func traversalWithValue(val string, traversalspec []string, filterspec FilterSpec, db *badger.DB, auditLevel string) (map[string][]map[string]interface{}, error) {

	//
	// Find the objects that contain the value
	//
	results := make(map[string][]map[string]interface{}, 0)
	targets := make(map[string]interface{}, 0)
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte(fmt.Sprintf("osp|%s", val))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			t := NewTriple(string(item.Key()))
			targets[t.S] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	//
	// follw the traversal spec for each of the objects
	//
	for target, _ := range targets {
		traversalResults, err := traversalWithId(target, traversalspec, filterspec, db, auditLevel)
		if err != nil {
			return nil, err
		}
		for k, result := range traversalResults {
			results[k] = append(results[k], result...)
		}
	}

	return results, nil

}
