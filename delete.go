// delete.go

package deep6

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	boom "github.com/tylertreat/BoomFilters"
)

//
// Deletes object with specified id, and all links to that object.
//
func (d6 *Deep6DB) Delete(id string) error {

	defer timeTrack(time.Now(), "Delete()")

	err := deleteWithID(id, d6.db, d6.rwb, d6.sbf, d6.AuditLevel, d6.folderPath)
	if err != nil {
		return errors.Wrap(err, "cannot delete object: "+id)
	}
	// flush the write buffer to apply deletes
	d6.rwb.Flush()
	// reset the writer for next use
	d6.rwb = d6.db.NewWriteBatch()

	return err

}

func deleteWithID(id string, db *badger.DB, wb *badger.WriteBatch, sbf *boom.ScalableBloomFilter, auditLevel, folderPath string) error {

	// see if object exists
	obj, err := findById(id, db)
	if err != nil {
		return err
	}
	psuedoStream := []map[string]interface{}{obj}

	//
	// pass object through the remove pipeline
	// by converting to a readable byte array
	// as if comming from a stream
	// add obj to []mapstringiface first then marshal
	//
	json, err := json.Marshal(psuedoStream)
	if err != nil {
		return errors.Wrap(err, "found id object but could not marshal as json.")
	}
	r := bytes.NewReader(json)

	// now run the remove sequence
	return runRemoveWithReader(db, wb, sbf, r, auditLevel, folderPath)

}
