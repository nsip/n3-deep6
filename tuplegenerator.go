// tuplegenerator.go

package deep6

import (
	"context"
	"fmt"
)

//
// Turns the orignal data into a list of
// subject:predicate:object Triples
//
// ctx - context used for pipeline management
// in - channel providing IngestData objects
//
func tupleGenerator(ctx context.Context, in <-chan IngestData) (
	<-chan IngestData,
	<-chan error, // emits errors encountered to the pipeline
	error) { // any error encountered when creating this component

	out := make(chan IngestData)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		for igd := range in {
			m := igd.RawData
			m = Flatten(m) // turn json into predicate:object pairs
			igd.RawData = m

			// create list of subject:predicate:object triples
			tuples := make([]Triple, 0)
			for k, v := range m {
				t := Triple{
					S: fmt.Sprintf("%s", igd.N3id),
					P: k,
					O: fmt.Sprintf("%v", v),
				}
				tuples = append(tuples, t)
			}

			igd.Triples = tuples

			select {
			case out <- igd: // pass the data on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}

		}

	}()

	return out, errc, nil
}

//
// Flatten takes a map of a json file and returns a new one where nested maps are replaced
// by dot-delimited keys.
//
func Flatten(m map[string]interface{}) map[string]interface{} {
	o := make(map[string]interface{})
	for k, v := range m {
		switch child := v.(type) {
		case map[string]interface{}: // nested map (object)
			nm := Flatten(child)
			for nk, nv := range nm {
				o[k+"."+nk] = nv
			}
		case []interface{}: // array
			for i, sv := range child {
				switch member := sv.(type) {
				case map[string]interface{}: // object inside array
					nm := Flatten(member)
					for nk, nv := range nm {
						key := fmt.Sprintf("%s.%d.%s", k, i, nk)
						o[key] = nv
					}
				default:
					key := fmt.Sprintf("%s.%d", k, i)
					o[key] = sv
				}
			}
		default:
			o[k] = v
		}
	}
	return o
}
