// query.go

package deep6

import (
	"fmt"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

//
// Finds all tuples for a given object
// reinflates into a json object
// and returns a map[string]interface{} representing the json
// object
//
// id - unique id of the object being searched for
//
func (d6 *Deep6DB) FindById(id string) (map[string][]map[string]interface{}, error) {

	defer timeTrack(time.Now(), "FindById()")

	m, err := findById(id, d6.db)
	if err != nil {
		return nil, err
	}

	// pass through filter to remove n3-specific properties
	// and to arrange result by type
	pseudoStream := make([]map[string]interface{}, 0)
	pseudoStream = append(pseudoStream, m)
	result, err := filterResults(pseudoStream, FilterSpec{})

	return result, err

}

func findById(id string, db *badger.DB) (map[string]interface{}, error) {

	jsonDoc, _ := sjson.SetBytes([]byte(""), "", "") // empty json doc to reinflate with tuples
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte(fmt.Sprintf("sop|%s|", id))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			t := NewTriple(string(item.Key()))
			jsonDoc, _ = sjson.SetBytes(jsonDoc, t.P, t.O)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	m, ok := gjson.ParseBytes(jsonDoc).Value().(map[string]interface{})
	if !ok {
		return nil, errors.New("could not convert json to map.")
	}

	//
	// internal findByID() does not use filter
	// to arrange content by type, that's done in the
	// d6 method above, as many internal services rely on
	// just getting the basic map from this method.
	//

	return m, nil

}

//
// Finds all objects of a specified type
// will be object name for SIF e.g. StudentPersonal
// or generic e.g. XAPI
// returns an array of map[string]interface{} containing the
// json objects
//
// typename - object type to search for
//
func (d6 *Deep6DB) FindByType(typename string, filterspec FilterSpec) (map[string][]map[string]interface{}, error) {

	defer timeTrack(time.Now(), "FindByType()")

	return findByType(typename, filterspec, d6.db)

}

func findByType(typename string, filterspec FilterSpec, db *badger.DB) (map[string][]map[string]interface{}, error) {

	results := make([]map[string]interface{}, 0)
	targets := make([]string, 0)
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte(fmt.Sprintf("pos|is-a|%s|", typename))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			t := NewTriple(string(item.Key()))
			targets = append(targets, t.S)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	for _, target := range targets {
		result, err := findById(target, db)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	//
	// strip n3 properties from objects
	// and collate results by type
	//
	return filterResults(results, filterspec)

}

//
// Finds all objects where a property has the given value.
// returns a map[string]interface{} representing the json object
//
// term - string value to search for
// supports partial (prefix) search e.g. jaqueline as opposed to jaqueline5@email.com
//
func (d6 *Deep6DB) FindByValue(term string, filterspec FilterSpec) (map[string][]map[string]interface{}, error) {

	defer timeTrack(time.Now(), "FindByValue()")

	return findByValue(term, filterspec, d6.db)
}

func findByValue(term string, filterspec FilterSpec, db *badger.DB) (map[string][]map[string]interface{}, error) {

	results := make([]map[string]interface{}, 0)
	targets := make(map[string]interface{}, 0)
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte(fmt.Sprintf("osp|%s", term))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			t := NewTriple(string(item.Key()))
			// fmt.Println(t)
			targets[t.S] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	for target, _ := range targets {
		result, err := findById(target, db)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	//
	// strip n3 properties from objects
	// and collate results by type
	//
	return filterResults(results, filterspec)
}

//
// Finds all objects where a predicate exists.
// More useful with xapi than other data.
//
// Predicate must be in dotted path form e.g.
// 'XAPI.verb' or
// 'StudentPersonal.PersonInfo.Name'
//
// returns a map[string]interface{} representing the json object
//
// predicate - string value to search for
// can be a partial predicate e.g.
// (full) StudentPersonal.PersonInfo.Name.FamilyName
// (partial) .FamilyName
//
// filterspec - optional filter for results
//
//
func (d6 *Deep6DB) FindByPredicate(predicate string, filterspec FilterSpec) (map[string][]map[string]interface{}, error) {

	defer timeTrack(time.Now(), "FindByPredicate()")

	return findByPredicate(predicate, filterspec, d6.db)
}

func findByPredicate(predicate string, filterspec FilterSpec, db *badger.DB) (map[string][]map[string]interface{}, error) {

	results := make([]map[string]interface{}, 0)
	targets := make(map[string]interface{}, 0) // use a map here to de-dupe, so user can pass part predicate
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte(fmt.Sprintf("pso|%s", predicate))
		fmt.Println(string(prefix))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			t := NewTriple(string(item.Key()))
			targets[t.S] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	for target, _ := range targets {
		result, err := findById(target, db)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	//
	// strip n3 properties from objects
	// and collate results by type
	//
	return filterResults(results, filterspec)
}
