//
// Deep6 is a simple embeddable hexastore graph database.
// To the core hexastore tuple index we add auto-generated
// links based on selected property values, and offer
// object traversals over linked data types.
//
// The use-case deep6 was creted for is to link data formats
// of different types in the education-technology space together
// so that user can retrieve, for example, all SIF, XAPI statements
// or abitrary json objects related to a student, a teacher or a school.
//
//
package deep6

import (
	"reflect"
	"testing"
)

func TestOpen(t *testing.T) {
	tests := []struct {
		name    string
		want    *Deep6DB
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name:    "TestOpen",
			want:    &Deep6DB{}, // only for make test runnable
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Open()
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Open() = %v, want %v", got, tt.want)
			}
		})
	}
}
