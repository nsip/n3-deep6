// ingestfile.go

package deep6

import (
	"io"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
)

//
// Load data into D6 from a file
//
func (d6 *Deep6DB) IngestFromFile(fname string) error {

	defer timeTrack(time.Now(), "IngestFromFile() "+fname)

	// open the data file
	f, err := os.Open(fname)
	if err != nil {
		return errors.Wrap(err, "cannot open data file: ")
	}

	return d6.IngestFromReader(f)

}

//
// Load data into D6 from an http request
//
func (d6 *Deep6DB) IngestFromHTTPRequest(r *http.Request) error {

	return d6.IngestFromReader(r.Body)

}

//
// Feed data in D6 from any io.Reader
//
func (d6 *Deep6DB) IngestFromReader(r io.Reader) error {

	err := runIngestWithReader(d6.db, d6.iwb, d6.sbf, r, d6.AuditLevel, d6.folderPath)
	if err != nil {
		return errors.Wrap(err, "error ingesting data from reader:")
	}
	// ensure the writer finishes
	d6.iwb.Flush()
	// reinstate the writer
	d6.iwb = d6.db.NewWriteBatch()

	return err

}

//
// Feed data into db from a channel providing json objects as
// byte slices - typically for us the iterator from crdt manager
// receiver
//
func (d6 *Deep6DB) IngestFromJSONChannel(c <-chan []byte) error {

	err := runIngestWithIterator(d6.db, d6.iwb, d6.sbf, c, d6.AuditLevel, d6.folderPath)
	if err != nil {
		return errors.Wrap(err, "error ingesting data from reader:")
	}
	// ensure the writer finishes
	d6.iwb.Flush()
	// reinstate the writer
	d6.iwb = d6.db.NewWriteBatch()

	return err

}
