// objectfilter.go

package deep6

import (
	"context"
	"strings"

	"github.com/pkg/errors"
)

//
// objectFilter applies the filter spec to
// each object passed through it.
//
func objectFilter(ctx context.Context, filterSpec FilterSpec, in <-chan map[string]interface{}) (
	<-chan map[string]interface{},
	<-chan error, // emits errors encountered to the pipeline
	error) {
	out := make(chan map[string]interface{})
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		for m := range in {

			objectType, ok := m["is-a"].(string)
			if !ok {
				errc <- errors.New("object with no type encountered in objectFilter():")
				return
			}

			filters, ok := filterSpec[objectType]
			filtered := false
			if ok { // there is a filter for this type
				m2 := Flatten(m)
				for k, v := range m2 {
					filtersPassed := 0
					for _, filter := range filters {
						if strings.Contains(k, filter.Predicate) && v == filter.TargetValue {
							filtersPassed++
						}
					}
					if len(filters) == filtersPassed {
						filtered = true // object passes filter
					}
				}
			} else {
				filtered = true // implicit if no filter in place
			}
			if !filtered {
				continue // move on to next object
			}

			select {
			case out <- m: // pass the data on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}
		}

	}()

	return out, errc, nil
}
