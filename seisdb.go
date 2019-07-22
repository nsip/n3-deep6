package deep6

// SeisDB is a Hexastore based on this paper
// http://www.vldb.org/pvldb/1/1453965.pdf

//
// This version is taken from the original:
//
// www.github.com/dahernan/seisdb
//
// changes here are db is now managed elsewhere
// and we've added the cross-datatype link tuples
//
// meaning that data of interest is automatically linked
// within the db, rather than being linked at query time.
//
//

import (
	"fmt"
	"strings"
)

// sextuples
// spo:dahernan:is-friend-of:agonzalezro
// sop:dahernan:agonzalezro:is-friend-of
// ops:agonzalezro:is-friend-of:dahernan
// osp:agonzalezro:dahernan:is-friend-of
// pso:is-friend-of:dahernan:agonzalezro
// pos:is-friend-of:agonzalezro:dahernan

type Triple struct {
	// subject
	S string
	// predicate
	P string
	// object
	O string
}

func NewTriple(tupla string) Triple {
	// parse this
	// spo:dahernan:is-friend-of:agonzalezro
	split := strings.SplitN(tupla, "|", 4)
	s := 1
	o := 2
	p := 3
	if len(split[0]) > 4 { // could be spo variant or spol
		return Triple{}
	}
	for index, ch := range split[0] {
		switch ch {
		case 's':
			s = index + 1
		case 'o':
			o = index + 1
		case 'p':
			p = index + 1
		}
	}
	return Triple{S: split[s], O: split[o], P: split[p]}

}

func (t Triple) Sextuple() []string {
	return []string{
		fmt.Sprintf("spo|%v|%v|%v", t.S, t.P, t.O),
		fmt.Sprintf("sop|%v|%v|%v", t.S, t.O, t.P),
		fmt.Sprintf("ops|%v|%v|%v", t.O, t.P, t.S),
		fmt.Sprintf("osp|%v|%v|%v", t.O, t.S, t.P),
		fmt.Sprintf("pso|%v|%v|%v", t.P, t.S, t.O),
		fmt.Sprintf("pos|%v|%v|%v", t.P, t.O, t.S),
	}
}

func (t Triple) SextupleLink() []string {
	return []string{
		fmt.Sprintf("spol|%v|%v|%v", t.S, t.P, t.O),
		fmt.Sprintf("sopl|%v|%v|%v", t.S, t.O, t.P),
		fmt.Sprintf("opsl|%v|%v|%v", t.O, t.P, t.S),
		fmt.Sprintf("ospl|%v|%v|%v", t.O, t.S, t.P),
		fmt.Sprintf("psol|%v|%v|%v", t.P, t.S, t.O),
		fmt.Sprintf("posl|%v|%v|%v", t.P, t.O, t.S),
	}
}
