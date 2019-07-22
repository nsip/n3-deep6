// traversalaudit.go

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
//           basic - list of object ids that matched the traversal-spec
//           high - basic and lis tof all graph links targeted
// in - channel providing IngestData objects
//
func traversalAuditSink(ctx context.Context, auditLevel string, in <-chan TraversalData) (
	<-chan error, // emits errors encountered to the pipeline
	error) { // any error encountered when creating this component

	errc := make(chan error, 1)

	go func() {
		defer close(errc)

		for td := range in {
			if auditLevel == "none" {
				continue
			}

			fmt.Println("===== Traversal Data ======")

			if auditLevel == "high" || auditLevel == "basic" {
				fmt.Println("Matches:")
				for k, v := range td.TraversalMatches {
					fmt.Printf("\n\t|%s (%s)", k, v)
				}
			}

			if auditLevel == "high" {
				fmt.Println()
				fmt.Println("Stage Targets:")
				for k, v := range td.TraversalStageTargets {
					fmt.Printf("\n\t|%s (%s)", k, v)
				}

			}

			fmt.Println("=====")

		}
	}()

	return errc, nil
}
