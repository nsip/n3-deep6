// sbf.go

package deep6

import (
	"log"
	"os"

	boom "github.com/tylertreat/BoomFilters"
)

//
// loads the scalable bloom filter from disk
//
// sbf is used to capture required graph links as data
// flows through ingest - removes need for any dependency
// on ordering of data ingest, all models can loaded
// in any order or in mixed input files/streams
//
func openSBF() *boom.ScalableBloomFilter {

	sbf := boom.NewDefaultScalableBloomFilter(0.01)
	sbfFile := "./db/featureLinks.sbf"
	f, err := os.Open(sbfFile)
	if err != nil {
		log.Println("cannot find sbf file, creating new sbf.")
	} else {
		size, err := sbf.ReadFrom(f)
		if err != nil {
			log.Println("cannot read sbf from file, using default: ", err)
		}
		log.Printf("sbf loaded from file: %d bytes.", size)
	}
	return sbf

}

//
// saves the supplied sbf to disk
//
func saveSBF(sbf *boom.ScalableBloomFilter) {
	sbfFile := "./db/featureLinks.sbf"
	f, err := os.Create(sbfFile)
	if err != nil {
		log.Println("cannot create sbf file:", err)
		return
	}
	size, err := sbf.WriteTo(f)
	if err != nil {
		log.Println("cannot save sbf to file: ", err)
		return
	}
	log.Printf("saved sbf to file: %d bytes.", size)

}
