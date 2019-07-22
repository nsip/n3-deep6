// ingestaudit.go

package deep6

import (
	"context"
	"fmt"
)

//
// terminator sink for ingest pipeline.
// will write audit information based on auditLevel
// useful for debugging; fast if piped to file, slower if
// printing to console.
//
// ctx - pipeline mangement context
// auditLevel - one of "none","basic", "high"
//           none - no output
//           basic - object id & type info for each object, link specs
//           high - classification, original raw data, link candidates, link triples
// in - channel providing IngestData objects
//
func ingestAuditSink(ctx context.Context, auditLevel string, in <-chan IngestData) (
	<-chan error, // emits errors encountered to the pipeline
	error) { // any error encountered when creating this component

	errc := make(chan error, 1)

	go func() {
		defer close(errc)

		for igd := range in {
			if auditLevel == "none" {
				continue
			}

			fmt.Println("=====")
			fmt.Println("object id: ", igd.N3id)
			fmt.Println("object type: ", igd.Type)

			if auditLevel == "high" {
				fmt.Println("raw:")
				for k, v := range igd.RawData {
					fmt.Printf("\n\t|%v|%v|%v", igd.N3id, k, v)
				}
			}

			if auditLevel == "basic" || auditLevel == "high" {
				fmt.Println()
				fmt.Println("link-specs: ", igd.LinkSpecs)
			}

			if auditLevel == "high" {
				fmt.Println()
				fmt.Println("link candidates:")
				for _, lc := range igd.LinkCandidates {
					fmt.Printf("\t|%v|%v|%v\n", lc.S, lc.P, lc.O)
				}
				fmt.Println("num link-candidates: ", len(igd.LinkCandidates))

				fmt.Println()
				fmt.Println("link triples:")
				for _, l := range igd.LinkTriples {
					fmt.Printf("\t\tlinking to: %s|%s|%s\n", l.S, l.P, l.O)
				}
				fmt.Println("num link-triples: ", len(igd.LinkTriples))
			}

			fmt.Println("=====")

		}
	}()

	return errc, nil
}
