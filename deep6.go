//
// Deep6 is a simple embeddable hexastore graph database.
// To the core hexastore tuple index we add auto-generated
// links based on selected property values, and offer
// object traversals over linked data types.
//
// The use-case deep6 was creted for is to link data formats
// of different types in the education-technology space together
// so that user can retrieve, for example, all SIF, XAPI statements
// or abitrary json objects related to a student, a teacher or a school.
//
//
package deep6

import (
	"log"
	"os"

	"github.com/dgraph-io/badger"
	boom "github.com/tylertreat/BoomFilters"
)

type Deep6DB struct {
	//
	// the underlying badger k/v store used by D6
	//
	db *badger.DB
	//
	// manages parallel async writing to db
	//
	iwb *badger.WriteBatch
	//
	// another 'writer' used for deletes
	//
	rwb *badger.WriteBatch
	//
	// sbf used to record links
	//
	sbf *boom.ScalableBloomFilter
	//
	// set level of audit ouput, one of: none, basic, high
	//
	AuditLevel string
	//
	// location of the database
	//
	folderPath string
}

//
// Open the database using the specified directory
// It will be created if it doesn't exist.
//
func OpenFromFile(folderPath string) (*Deep6DB, error) {

	log.Println("opening d6 database...")

	err := os.MkdirAll(folderPath, os.ModePerm)
	if err != nil {
		return nil, err
	}

	options := badger.DefaultOptions(folderPath)
	// options = options.WithSyncWrites(false) // speed optimisation if required
	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}
	iwb := db.NewWriteBatch()
	rwb := db.NewWriteBatch()

	sbf := openSBF(folderPath)

	err = createDefaultConfig(folderPath)
	if err != nil {
		return nil, err
	}

	log.Println("...d6 database open")

	return &Deep6DB{
		db:         db,
		iwb:        iwb,
		rwb:        rwb,
		sbf:        sbf,
		AuditLevel: "high",
		folderPath: folderPath}, nil
}

//
// Open a d6db will use the local path
// ./db/d6 by default
//
func Open() (*Deep6DB, error) {

	// if no filename provided will create locally
	return OpenFromFile("./db/d6")

}

//
// shuts down the and ensures all writes are
// committed.
//
func (d6 *Deep6DB) Close() {
	log.Println("closing d6 database...")

	err := d6.iwb.Flush()
	if err != nil {
		log.Println("error flushing ingest writebatch: ", err)
	}
	err = d6.rwb.Flush()
	if err != nil {
		log.Println("error flushing delete writebatch: ", err)
	}

	err = d6.db.Close()
	if err != nil {
		log.Println("error closing datastore:", err)
	}

	log.Println("saving sbf....")
	saveSBF(d6.sbf, d6.folderPath)
	log.Println("...sbf saved.")
	log.Println("...d6 database closed")

}
