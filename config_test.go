// config.go

package deep6

import "testing"

func Test_createDefaultConfig(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "OK",
			args: args{
				filePath: "./",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := createDefaultConfig(tt.args.filePath); (err != nil) != tt.wantErr {
				t.Errorf("createDefaultConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
