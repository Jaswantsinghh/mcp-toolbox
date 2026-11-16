// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgresliststoredprocedure_test

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/mcp-toolbox/internal/server"
	"github.com/googleapis/mcp-toolbox/internal/testutils"
	"github.com/googleapis/mcp-toolbox/internal/tools"
	"github.com/googleapis/mcp-toolbox/internal/tools/postgres/postgresliststoredprocedure"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeRows implements the subset of pgx.Rows used by collectRows.
type fakeRows struct {
	pgx.Rows
	fields []pgconn.FieldDescription
	rows   [][]any
	err    error
	pos    int
}

func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return r.fields }

func (r *fakeRows) Next() bool {
	if r.pos >= len(r.rows) {
		return false
	}
	r.pos++
	return true
}

func (r *fakeRows) Values() ([]any, error) { return r.rows[r.pos-1], nil }

func (r *fakeRows) Err() error { return r.err }

func TestCollectRows(t *testing.T) {
	fields := []pgconn.FieldDescription{{Name: "schema_name"}, {Name: "name"}}
	rows := [][]any{{"public", "proc_a"}, {"public", "proc_b"}}

	t.Run("returns rows", func(t *testing.T) {
		got, err := postgresliststoredprocedure.CollectRows(&fakeRows{fields: fields, rows: rows})
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		want := []map[string]any{
			{"schema_name": "public", "name": "proc_a"},
			{"schema_name": "public", "name": "proc_b"},
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("incorrect rows: diff %v", diff)
		}
	})

	t.Run("returns iteration error", func(t *testing.T) {
		iterErr := errors.New("canceling statement due to statement timeout")
		got, err := postgresliststoredprocedure.CollectRows(&fakeRows{fields: fields, rows: rows[:1], err: iterErr})
		if err == nil {
			t.Fatalf("expected error, got rows %v", got)
		}
		if !errors.Is(err, iterErr) {
			t.Fatalf("expected error to wrap %q, got %q", iterErr, err)
		}
	})
}

func TestParseFromYamlPostgresListStoredProcedure(t *testing.T) {
	ctx, err := testutils.ContextWithNewLogger()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	tcs := []struct {
		desc string
		in   string
		want server.ToolConfigs
	}{
		{
			desc: "basic example",
			in: `
			kind: tool
			name: example_tool
			type: postgres-list-stored-procedure
			source: my-postgres-instance
			description: some description
			authRequired:
				- my-google-auth-service
				- other-auth-service
			`,
			want: server.ToolConfigs{
				"example_tool": postgresliststoredprocedure.Config{
					ConfigBase: tools.ConfigBase{
						Name:         "example_tool",
						Description:  "some description",
						AuthRequired: []string{"my-google-auth-service", "other-auth-service"},
					},
					Type:   "postgres-list-stored-procedure",
					Source: "my-postgres-instance",
				},
			},
		},
		{
			desc: "basic example",
			in: `
			kind: tool
			name: example_tool
			type: postgres-list-stored-procedure
			source: my-postgres-instance
			description: some description
			`,
			want: server.ToolConfigs{
				"example_tool": postgresliststoredprocedure.Config{
					ConfigBase: tools.ConfigBase{
						Name:         "example_tool",
						Description:  "some description",
						AuthRequired: []string{},
					},
					Type:   "postgres-list-stored-procedure",
					Source: "my-postgres-instance",
				},
			},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			_, _, _, got, _, _, _, _, err := server.UnmarshalPrimitiveConfig(ctx, testutils.FormatYaml(tc.in))
			if err != nil {
				t.Fatalf("unable to unmarshal: %s", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("incorrect parse: diff %v", diff)
			}
		})
	}

}
