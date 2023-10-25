package parser

import "testing"

func TestParseQuery(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test case1",
			args: args{
				query: "SELECT c FROM users WHERE c < 1",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ParseQuery(tt.args.query); (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
