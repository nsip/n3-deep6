// removeaudit.go

package deep6

import (
	"context"
	"fmt"
)

//
// terminator sink for remove pipeline.
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
func removeAuditSink(ctx context.Context, auditLevel string, in <-chan IngestData) (
	<-chan error, // emits errors encountered to the pipeline
	error) { // any error encountered when creating this component

	errc := make(chan error, 1)

	go func() {
		defer close(errc)

		for igd := range in {
			if auditLevel == "none" {
				continue
			}

			fmt.Println("====== REMOVAL AUDIT ======")
			fmt.Println("=====")
			fmt.Println("object id: ", igd.N3id)
			fmt.Println("object type: ", igd.Type)

			if auditLevel == "high" {
				fmt.Println("tuples removed:")
				for k, v := range igd.RawData {
					fmt.Printf("\n\t|%v|%v|%v", igd.N3id, k, v)
				}
			}

			if auditLevel == "high" {
				fmt.Println()
				fmt.Println("links removed:")
				for _, l := range igd.LinkTriples {
					fmt.Printf("\t\tlink removed to: %s|%s|%s\n", l.S, l.P, l.O)
				}
				fmt.Println("num link-triples removed: ", len(igd.LinkTriples))
			}

			fmt.Println("=====")

		}
	}()

	return errc, nil
}
