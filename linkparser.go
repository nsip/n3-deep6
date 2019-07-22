// linkparser.go

package deep6

import (
	"context"
	"strings"

	boom "github.com/tylertreat/BoomFilters"
)

//
// parses inbound object for candidate properties to link into the graph
//
// ctx - pipeline management context
// sbf - bloom filter used to capture required link fields between objects
// in - channel providing IngestData objects
//
func linkParser(ctx context.Context, sbf *boom.ScalableBloomFilter, in <-chan IngestData) (
	<-chan IngestData, // new list of triples also containing links
	<-chan error, // emits errors encountered to the pipeline
	error) { // returns any errors when creating this component

	out := make(chan IngestData)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		for igd := range in {
			//
			// extract the object (O:) members of any tuples that match the linking predicate
			//
			// these are links that should be made accessible to the graph
			// as they've been specified as linkable properties
			// so we add them to the bloom filter
			//
			linkTraces := make([]string, 0)
			for _, t := range igd.Triples {
				for _, s := range igd.LinkSpecs {
					if strings.Contains(t.P, s) {
						linkTrace := t.O
						linkTraces = append(linkTraces, linkTrace)
					}
				}
			}
			//
			// if igd has a pseudo-unique key, add it to the traces
			//
			if len(igd.Unique) > 0 {
				linkTraces = append(linkTraces, igd.Unique)
			}
			// add specified link attributes to sbf
			for _, lt := range linkTraces {
				sbf.Add([]byte(lt))
			}

			// now do second pass to see if object contains any links
			// of interest to others or specified links
			//
			// did some other object leave a linkTrace that we should
			// observe becasue it is valid for our data properties
			//
			links := make([]Triple, 0)
			for _, t := range igd.Triples {
				if t.O == igd.N3id {
					continue // ignore self-links
				}
				// see if anyone has registered an interest in this tuple's value
				if sbf.Test([]byte(t.O)) {
					link := t
					links = append(links, link)
				}
			}

			igd.LinkCandidates = links

			select {
			case out <- igd: // pass the data on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}

		}

	}()

	return out, errc, nil

}
