// filter.go

package deep6

// data strucure
//
// keyname is object type
// accesses array of key/vale string pairs, where
// key: predicte string (can be part predicate) in dotted form e.g. PersonInfo.FamilyName
//    or simply .FamilyName
// value: the value to use as a filter, if the value matches the
// value of the predicate in the object the object will be returned in results
//
type FilterSpec map[string][]Filter
type Filter struct {
	Predicate   string
	TargetValue string
}
