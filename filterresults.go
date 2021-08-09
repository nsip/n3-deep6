// filterresults.go

package deep6

import (
	"context"

	"github.com/pkg/errors"
)

//
// Filter to be applied on a set of search results, from any of the
// findBy() methods reurns a json map of the results
// with results seaprated by type
//
//
func filterResults(searchResults []map[string]interface{}, filterSpec FilterSpec) (map[string][]map[string]interface{}, error) {

	results := make(map[string][]map[string]interface{})

	// monitor classifier for errors
	var errcList []<-chan error
	// set up a context to manage pipeline
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	sourceOut, errc, err := resultsSource(ctx, searchResults)
	if err != nil {
		return nil, errors.Wrap(err, "Error: cannot create results-source component:")
	}
	errcList = append(errcList, errc)

	filterOut, errc, err := objectFilter(ctx, filterSpec, sourceOut)
	if err != nil {
		return nil, errors.Wrap(err, "Error: cannot create object-filter component:")
	}
	errcList = append(errcList, errc)

	tidyOut, errc, err := resultsTidy(ctx, &results, filterOut)
	if err != nil {
		return nil, errors.Wrap(err, "Error: cannot create object-tidy component:")
	}
	errcList = append(errcList, errc)

	// sink for pipeline
	for igd := range tidyOut {
		_ = igd
	}

	// monitor progress
	err = WaitForPipeline(errcList...)

	return results, err

}
