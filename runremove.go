// runremove.go

package deep6

import (
	"context"
	"io"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"
	boom "github.com/tylertreat/BoomFilters"
)

//
// Runs the remove pipleine to delete data.
//
//
// The generation of tuples & links is deterministic
// same tuple in will result in same links and tuples
// so remove starts with an object reader,
// typically provided upstream by e.g. FindByID()
// but can also be a stream or file.
//
// Then goes through the ingest process; generates
// all possible tuples, generates all poosible links
// but rather than writing them, they are now deleted
//
// Needs db and wb as these are presumed to be in use
// by other pipelines or application features.
//
// eg. db/wb are used here for removing data
// but db can also be in use to support queries in parallel.
//
// db - instance of a badger db
// wb - badger.WriteBatch, a fast write manager provided by the db
// sbf - bloom filter used to capture required graph links
// r - the io.Reader (file, http body etc.) to be ingested
// auditLevel - one of: none, basic, high
//
func runRemoveWithReader(db *badger.DB, wb *badger.WriteBatch, sbf *boom.ScalableBloomFilter, r io.Reader, auditLevel string) error {

	// set up a context to manage ingest pipeline
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// monitor all error channels
	var errcList []<-chan error

	//
	// build the pipleine by connecting all stages
	//
	jsonOut, errc, err := jsonReaderSource(ctx, r)
	if err != nil {
		return errors.Wrap(err, "Error: cannot create json-reader source component: ")
	}
	errcList = append(errcList, errc)

	classOut, errc, err := objectClassifier(ctx, jsonOut)
	if err != nil {
		return errors.Wrap(err, "Error: cannot create object-classifier component: ")
	}
	errcList = append(errcList, errc)

	genOut, errc, err := tupleGenerator(ctx, classOut)
	if err != nil {
		return errors.Wrap(err, "Error: cannot create tuple-generator component: ")
	}
	errcList = append(errcList, errc)

	linkerOut, errc, err := linkParser(ctx, sbf, genOut)
	if err != nil {
		return errors.Wrap(err, "Error: cannot create link-parser component: ")
	}
	errcList = append(errcList, errc)

	builderOut, errc, err := linkBuilder(ctx, db, wb, linkerOut)
	if err != nil {
		return errors.Wrap(err, "Error: cannot create link-builder component: ")
	}
	errcList = append(errcList, errc)

	lremoverOut, errc, err := linkRemover(ctx, wb, builderOut)
	if err != nil {
		return errors.Wrap(err, "Error: cannot create link-writer component: ")
	}
	errcList = append(errcList, errc)

	tremoverOut, errc, err := tripleRemover(ctx, wb, lremoverOut)
	if err != nil {
		return errors.Wrap(err, "Error: cannot create triple-writer component: ")
	}
	errcList = append(errcList, errc)

	errc, err = removeAuditSink(ctx, auditLevel, tremoverOut)
	if err != nil {
		return errors.Wrap(err, "Error: cannot create audit-sink component: ")
	}
	errcList = append(errcList, errc)

	// monitor progress
	err = WaitForPipeline(errcList...)

	return err

}
