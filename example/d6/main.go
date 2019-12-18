package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	deep6 "github.com/nsip/n3-deep6"
)

func main() {

	// pass a file of json data to read on the commandline
	fname := flag.String("data", "", "name of data file")
	auditLevel := flag.String("auditLevel", "none", "one of: none, basic, high")
	flag.Parse()

	db, err := deep6.Open()
	if err != nil {
		log.Fatal("cannot open datastore.", err)
	}
	defer db.Close()

	db.AuditLevel = *auditLevel

	// load data from file if provided on command line
	if len(*fname) > 0 {
		err = db.IngestFromFile(*fname)
		if err != nil {
			log.Println("error processing data file: ", err)
		}
	}

	//
	// load sample data
	//
	sampleDataPaths := []string{
		"./sample_data/naplan/sif.json",
		"./sample_data/subjects/subjects.json",
		"./sample_data/lessons/lessons.json",
		"./sample_data/curriculum/overview.json",
		"./sample_data/curriculum/content.json",
		"./sample_data/sif/sif.json",
		"./sample_data/xapi/xapi.json",
	}

	for _, path := range sampleDataPaths {
		err = db.IngestFromFile(path)
		if err != nil {
			log.Println("error processing data file: ", err)
		}
	}

	var prettyJSON bytes.Buffer
	var jsonBytes []byte

	//
	// FindByID()
	//
	id := "738F4DF5-949F-4380-8186-8252440A6F6F" // schoolinfo
	// id := "CCCEE62C-DEFE-4397-B22B-1AA2888E70DA" // tt subject
	// id := "abfhskdjfjfdksll" // does not exist
	// id := "F3B29F64-E30C-4334-8DA6-15DA41225041" // teaching group
	fmt.Println("=== FindById() =====")
	fmt.Println("\tsearching for: " + id)
	resultsById, err := db.FindById(id)
	if err != nil {
		fmt.Println("Cannot find object by id:" + id)
		// return
	}
	//
	// pretty print the json query response
	//
	jsonBytes, err = json.Marshal(resultsById)
	if err != nil {
		log.Fatal("ERROR: json marshalling error: ", err)
	}
	err = json.Indent(&prettyJSON, jsonBytes, "", "    ")
	fmt.Println(string(prettyJSON.Bytes()))
	fmt.Println("==========")
	prettyJSON.Reset()

	//
	// FindByType()
	//
	typename := "StudentPersonal"
	// typename := "StudentAttendanceTimeList"
	// typename := "SYLLABUS"
	// typename := "XAPI"
	// typename := "NAPTest"
	// typename := "GradingAssignment"
	fmt.Println("=== FndByType() ====")
	fmt.Println("\tsearching for: " + typename)
	resultsByType, err := db.FindByType(typename, deep6.FilterSpec{})
	fmt.Println("results size: ", len(resultsByType[typename]))
	if err != nil {
		fmt.Println("Cannot find objects by type:" + typename)
		return
	}
	//
	// pretty print the json query response
	//
	jsonBytes, err = json.Marshal(resultsByType)
	if err != nil {
		log.Fatal("ERROR: json marshalling error: ", err)
	}
	err = json.Indent(&prettyJSON, jsonBytes, "", "    ")
	fmt.Println(string(prettyJSON.Bytes()))
	fmt.Println("==========")
	prettyJSON.Reset()

	//
	// FindByValue()
	//
	// val := "http://example.com/assignments/Mathematics-8-1-A:2"
	// val := "History"
	// val := "donna84@dodgit.com"
	// val := "history-5"
	val := "gdpbw581"
	// val := "Nellie Harris"
	// val := "SIF.PropertyLink"
	// val := "99CA49C1-75A7-4F56-A0C2-F1E4B3CAB1BC" // xapi object
	// val := "0054EB5F-07E6-4A26-84FA-2ADDBF5D84E9" // sif teaching group
	// val := "nwkoe858" // sif student localid
	// val := "Jacquelin Bailey" // sif student localid
	// val := "jacquelin" // sif email, should link to xapi and StudentPersonal
	fmt.Println("==== FindByValue() ====")
	fmt.Println("\t searching for: " + val)
	resultsByVal, err := db.FindByValue(val, deep6.FilterSpec{})
	if err != nil {
		fmt.Println("Cannot find objects with value:" + val)
		return
	}
	fmt.Println("results size: ", len(resultsByVal))
	//
	// pretty print the json query response
	//
	jsonBytes, err = json.Marshal(resultsByVal)
	if err != nil {
		log.Fatal("ERROR: json marshalling error: ", err)
	}
	err = json.Indent(&prettyJSON, jsonBytes, "", "    ")
	fmt.Println(string(prettyJSON.Bytes()))
	fmt.Println("==========")
	prettyJSON.Reset()

	//
	// FindByPredicate()
	//
	pred := "StudentPersonal.PersonInfo.Name"
	// pred := "SubjectLink.learning_area"
	// pred := "verb"
	// pred := "actor"
	// val := "references"
	fmt.Println("==== FindByPredicate() ====")
	fmt.Println("\t searching for: " + pred)
	resultsByPred, err := db.FindByPredicate(pred, deep6.FilterSpec{})
	if err != nil {
		fmt.Println("Cannot find objects by predicate:" + pred)
		return
	}
	fmt.Println("results size: ", len(resultsByPred))
	//
	// pretty print the json query response
	//
	jsonBytes, err = json.Marshal(resultsByPred)
	if err != nil {
		log.Fatal("ERROR: json marshalling error: ", err)
	}
	err = json.Indent(&prettyJSON, jsonBytes, "", "    ")
	fmt.Println(string(prettyJSON.Bytes()))
	fmt.Println("==========")
	prettyJSON.Reset()

	//
	// TraversalWithId() and Traversal Filters
	//
	startid := "A4F0069E-D3B8-4822-BDD9-4D649E2A47FD" // staff pesonal refid
	// id := "99CA49C1-75A7-4F56-A0C2-F1E4B3CAB1BC" //xapi
	// id := "82656FA0-17B6-42BF-9915-487360FDF361" //sif student personal - links to TG
	// id := "3E7F40EE-4298-43EE-9659-B5A5A1DF6FF9" // sif student with -links to TG and attendance
	// id := "0054EB5F-07E6-4A26-84FA-2ADDBF5D84E9" // sif teaching group
	// id := "1C8FC460-9354-4F1E-854E-598F04C60D1E" // xapi object
	// id := "Julius Alexander" // xapi name
	// id := "nwkoe858"               // sif student localid
	// id := "jacquelin5@spambob.com" // sif email, should link to xapi and StudentPersonal
	// id := "lzrlb501" // staff personal localid
	fmt.Println("==== TraversalWithId() and Filter ====")
	fmt.Println("\t starting traversal at: " + startid)

	// // create traversal as json
	// traversalDef := `{"TraversalSpec":[
	// 	"StaffPersonal",
	// 	"TeachingGroup",
	// 	"GradingAssignment",
	// 	"Property.Link",
	// 	"XAPI",
	// 	"Property.Link",
	// 	"Subject",
	// 	"Unique.Link",
	// 	"Syllabus",
	// 	"Unique.Link",
	// 	"Lesson"
	// ]}`

	traversalDef := `{"TraversalSpec":[
		"StaffPersonal",
		"TeachingGroup",
		"GradingAssignment",
		"XAPI",
		"Subject",
	 	"Unique.Link",
	 	"Syllabus",
	 	"Unique.Link",
	 	"Lesson"
	]}`

	fmt.Println("Traversal Spec:\n", traversalDef)
	var jsonTraversal deep6.Traversal
	if err := json.Unmarshal([]byte(traversalDef), &jsonTraversal); err != nil {
		panic(err)
	}
	// create filterspec as json
	filterDef := `{
		"XAPI":[{
			"Predicate":"actor.name","TargetValue":"Albert Lombardi"
		}],
		"TeachingGroup":[{
			"Predicate":".LocalId","TargetValue":"2018-History-8-1-A"
		}]
	}`
	fmt.Println("Filter Spec:\n", filterDef)
	var jsonFilterSpec deep6.FilterSpec
	if err := json.Unmarshal([]byte(filterDef), &jsonFilterSpec); err != nil {
		panic(err)
	}
	resultsTraversalId, err := db.TraversalWithId(startid, jsonTraversal, jsonFilterSpec)
	if err != nil {
		fmt.Println("Cannot follow traversal from:" + startid)
		return
	}
	fmt.Println("results size: ", len(resultsTraversalId))
	//
	// pretty print the json query response
	//
	jsonBytes, err = json.Marshal(resultsTraversalId)
	if err != nil {
		log.Fatal("ERROR: json marshalling error: ", err)
	}
	err = json.Indent(&prettyJSON, jsonBytes, "", "    ")
	fmt.Println(string(prettyJSON.Bytes()))
	fmt.Println("==========")
	prettyJSON.Reset()

	//
	// Delete()
	//
	// delete the history teaching group from the db
	fmt.Println("==== deleting teaching group ====")
	deleteid := "6718F449-16FE-4B8E-9B21-E0319CA65C1B"
	err = db.Delete(deleteid)
	if err != nil {
		fmt.Println("Cannot delete object by id:" + deleteid)
		return
	}
	fmt.Println("   deleted: " + deleteid)
	fmt.Println("==========")

	//
	// TraversalWithId() & Filters post delete
	//
	// re-run the traversal, should now stop at
	// StaffPersonal as teaching group is removed.
	fmt.Println("=== traversal post-delete teaching group ====")
	fmt.Println("=== should only return staff personal ====")
	fmt.Println("Traversal Spec:\n", traversalDef)
	fmt.Println("Filter Spec:\n", filterDef)
	resultsTraversalDelete, err := db.TraversalWithId(startid, jsonTraversal, jsonFilterSpec)
	if err != nil {
		fmt.Println("Cannot follow traversal from:" + startid)
		return
	}
	fmt.Println("results size: ", len(resultsTraversalDelete))
	//
	// pretty print the json query response
	//
	jsonBytes, err = json.Marshal(resultsTraversalDelete)
	if err != nil {
		log.Fatal("ERROR: json marshalling error: ", err)
	}
	err = json.Indent(&prettyJSON, jsonBytes, "", "    ")
	fmt.Println(string(prettyJSON.Bytes()))
	fmt.Println("==========")
	prettyJSON.Reset()

	//
	// Ordering...
	// ordering of sif objects, check reverse lookup
	// add sif objects out of order & check links;
	// in sample file schoolinfo comes after
	// schoolcourseinfo objects that reference it
	// so this traversal will only work if reverse
	// links are correct.
	//
	fmt.Println("==== ordering check ====")
	schoolId := "738F4DF5-949F-4380-8186-8252440A6F6F"
	fmt.Println("==== TraversalWithId() ====")
	fmt.Println("\t starting traversal at: " + schoolId)
	orderedTraversalDef := `{"TraversalSpec":[
		"SchoolInfo",
		"SchoolCourseInfo"
	]}`
	fmt.Println("=== should return 1 schoolinfo 8 schoolcourseinfos ====")
	fmt.Println("Traversal Spec:\n", orderedTraversalDef)
	var jsonOrderedTraversal deep6.Traversal
	if err := json.Unmarshal([]byte(orderedTraversalDef), &jsonOrderedTraversal); err != nil {
		panic(err)
	}
	resultsOrderedTraversal, err := db.TraversalWithId(schoolId, jsonOrderedTraversal, deep6.FilterSpec{})
	if err != nil {
		fmt.Println("Cannot follow traversal from:" + startid)
		return
	}
	fmt.Println("results size: ", len(resultsOrderedTraversal))
	//
	// pretty print the json query response
	//
	jsonBytes, err = json.Marshal(resultsOrderedTraversal)
	if err != nil {
		log.Fatal("ERROR: json marshalling error: ", err)
	}
	err = json.Indent(&prettyJSON, jsonBytes, "", "    ")
	fmt.Println(string(prettyJSON.Bytes()))
	fmt.Println("==========")
	prettyJSON.Reset()

	//
	// TraversalWithValue()
	//
	//
	// tval := "Julius Alexander" // xapi name
	tval := "nwkoe858" // sif student localid
	// tval := "jacquelin5@spambob.com" // sif email, should link to xapi and StudentPersonal
	// tval := "lzrlb501" // staff personal localid
	fmt.Println("==== TraversalWithValue() ====")
	fmt.Println("\t starting traversal at: " + tval)
	traversalDef = `{"TraversalSpec":[
		"StudentPersonal",
		"XAPI"
	]}`

	fmt.Println("Traversal Spec:\n", traversalDef)
	if err := json.Unmarshal([]byte(traversalDef), &jsonTraversal); err != nil {
		panic(err)
	}
	resultsTraversalWithVal, err := db.TraversalWithValue(tval, jsonTraversal, deep6.FilterSpec{})
	if err != nil {
		fmt.Println("Cannot follow traversal from id:" + tval)
		return
	}
	fmt.Println("results size: ", len(resultsTraversalWithVal))
	//
	// pretty print the json query response
	//
	jsonBytes, err = json.Marshal(resultsTraversalWithVal)
	if err != nil {
		log.Fatal("ERROR: json marshalling error: ", err)
	}
	err = json.Indent(&prettyJSON, jsonBytes, "", "    ")
	fmt.Println(string(prettyJSON.Bytes()))
	fmt.Println("==========")
	prettyJSON.Reset()

	//
	// TraversalWithValue() for NAPLAN
	// for  given student & given test (filter)
	// find all tests, items, testlets, responses
	// for that student.
	//
	//
	// tval := "Julius Alexander" // xapi name
	// tnaplanval := "x00101463" // test item localid
	tnaplanval := "330590347" // student local id
	// tval := "jacquelin5@spambob.com" // sif email, should link to xapi and StudentPersonal
	// tval := "lzrlb501" // staff personal localid
	fmt.Println("==== TraversalWithValue() For  NAPLAN data ====")
	fmt.Println("\t starting traversal at: " + tnaplanval)
	traversalDef = `{"TraversalSpec":[
      "StudentPersonal",
      "NAPEventStudentLink",
      "NAPTest",
      "NAPStudentResponseSet",
      "NAPTestlet",
      "NAPTestItem"
  	]}`

	fmt.Println("Traversal Spec:\n", traversalDef)
	if err := json.Unmarshal([]byte(traversalDef), &jsonTraversal); err != nil {
		panic(err)
	}

	// create filterspec as json
	naplanFilterDef := `{
		"NAPEventStudentLink":[{
			"Predicate":".NAPTestRefId","TargetValue":"2d98a118-1b42-417a-80e3-5ffb6c4b7042"
		}]
	}`
	fmt.Println("Filter Spec:\n", naplanFilterDef)
	if err := json.Unmarshal([]byte(naplanFilterDef), &jsonFilterSpec); err != nil {
		panic(err)
	}

	resultsNAPLANTraversalWithVal, err := db.TraversalWithValue(tnaplanval, jsonTraversal, jsonFilterSpec)
	if err != nil {
		fmt.Println("Cannot follow traversal from id:" + tnaplanval)
		return
	}
	fmt.Println("results size: ", len(resultsNAPLANTraversalWithVal))
	//
	// pretty print the json query response
	//
	jsonBytes, err = json.Marshal(resultsNAPLANTraversalWithVal)
	if err != nil {
		log.Fatal("ERROR: json marshalling error: ", err)
	}
	err = json.Indent(&prettyJSON, jsonBytes, "", "    ")
	fmt.Println(string(prettyJSON.Bytes()))
	fmt.Println("==========")
	prettyJSON.Reset()

	fmt.Println("all queries processed")
	fmt.Println("==========")

}
