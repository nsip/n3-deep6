// filter.go

package deep6

// data strucure
//
// Predicate: predicte string (can be part predicate) in dotted form e.g. PersonInfo.FamilyName
//    or simply .FamilyName
// TargetValue: the value to use as a filter, if the value matches the
// value of the predicate in the object the object will be returned in results
//
type FilterSpec map[string][]Filter
type Filter struct {
	Predicate   string
	TargetValue string
}
