package parser

import (
	"distributed-sqlite/types"
	"testing"
)

func TestParseQuery(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		name            string
		args            args
		wantParsedQuery *types.Query
		wantErr         bool
	}{
		{
			name: "select with equality id",
			args: args{query: "select * from users where id = '1'"},
			wantParsedQuery: &types.Query{
				Type:   "select",
				Tables: []string{"users"},
				PKey:   1,
				HasOr:  false,
			},
			wantErr: false,
		},
		{
			name: "select with multiple tables",
			args: args{query: "select * from users, users2"},
			wantParsedQuery: &types.Query{
				Type:   "select",
				Tables: []string{"users", "users2"},
				PKey:   -1,
				HasOr:  false,
			},
			wantErr: false,
		},
		{
			name: "select with multiple tables",
			args: args{query: "select * from users u JOIN users2 u2"},
			wantParsedQuery: &types.Query{
				Type:   "select",
				Tables: []string{"users", "users2"},
				PKey:   -1,
				HasOr:  false,
			},
			wantErr: false,
		},
		{
			name: "select with inequality id",
			args: args{query: "select * from users where id <= '1'"},
			wantParsedQuery: &types.Query{
				Type:   "select",
				Tables: []string{"users"},
				PKey:   -1,
				HasOr:  false,
			},
			wantErr: false,
		},
		{
			name: "select without id",
			args: args{query: "select * from users where ix = '1'"},
			wantParsedQuery: &types.Query{
				Type:   "select",
				Tables: []string{"users"},
				PKey:   -1,
				HasOr:  false,
			},
			wantErr: false,
		},
		{
			name: "select with id but with or",
			args: args{query: "select * from users where id = '1' or ix = '1'"},
			wantParsedQuery: &types.Query{
				Type:   "select",
				Tables: []string{"users"},
				PKey:   1,
				HasOr:  true,
			},
			wantErr: false,
		},
		{
			name: "insert",
			args: args{query: "insert into users values (1,2,3)"},
			wantParsedQuery: &types.Query{
				Type: "insert",
				PKey: 1,
			},
			wantErr: false,
		},
		{
			name: "update with equality id",
			args: args{query: "update users set id = '2' where id = '1'"},
			wantParsedQuery: &types.Query{
				Type:  "update",
				PKey:  1,
				HasOr: false,
			},
			wantErr: false,
		},
		{
			name: "delete with equality id",
			args: args{query: "delete from users where id = '1'"},
			wantParsedQuery: &types.Query{
				Type:  "delete",
				PKey:  1,
				HasOr: false,
			},
			wantErr: false,
		},
		{
			name:            "bad query",
			args:            args{query: "this is a bad query"},
			wantParsedQuery: nil,
			wantErr:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotParsedQuery, err := ParseQuery(tt.args.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr == true {
				return
			}
			if gotParsedQuery.Type == "select" {
				if gotParsedQuery.Type != tt.wantParsedQuery.Type {
					t.Errorf("Got type %v, want type %v", gotParsedQuery.Type, tt.wantParsedQuery.Type)
				}
				if len(gotParsedQuery.Tables) != len(tt.wantParsedQuery.Tables) {
					t.Errorf("Got length %d, want length %d", len(gotParsedQuery.Tables), len(tt.wantParsedQuery.Tables))
				}
				if gotParsedQuery.PKey != tt.wantParsedQuery.PKey {
					t.Errorf("Got %d, want %d", gotParsedQuery.PKey, tt.wantParsedQuery.PKey)
				}
				if gotParsedQuery.HasOr != tt.wantParsedQuery.HasOr {
					t.Errorf("Got %v, want %v", gotParsedQuery.HasOr, tt.wantParsedQuery.HasOr)
				}
			} else if gotParsedQuery.Type == "update" || gotParsedQuery.Type == "delete" {
				if gotParsedQuery.Type != tt.wantParsedQuery.Type {
					t.Errorf("Got type %v, want type %v", gotParsedQuery.Type, tt.wantParsedQuery.Type)
				}
				if gotParsedQuery.PKey != tt.wantParsedQuery.PKey {
					t.Errorf("Got %d, want %d", gotParsedQuery.PKey, tt.wantParsedQuery.PKey)
				}
				if gotParsedQuery.HasOr != tt.wantParsedQuery.HasOr {
					t.Errorf("Got %v, want %v", gotParsedQuery.HasOr, tt.wantParsedQuery.HasOr)
				}
			} else {
				if gotParsedQuery.Type != tt.wantParsedQuery.Type {
					t.Errorf("Got type %v, want type %v", gotParsedQuery.Type, tt.wantParsedQuery.Type)
				}
				if gotParsedQuery.PKey != tt.wantParsedQuery.PKey {
					t.Errorf("Got %d, want %d", gotParsedQuery.PKey, tt.wantParsedQuery.PKey)
				}
			}

		})
	}
}
