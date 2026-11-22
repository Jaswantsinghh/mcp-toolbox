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

package bigtable

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/googleapis/mcp-toolbox/internal/testutils"
	"github.com/googleapis/mcp-toolbox/tests"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// The Bigtable emulator shipped with the gcloud CLI. It is the same in-memory
// server as cloud.google.com/go/bigtable/bttest and only implements the data
// plane and the table admin API, so this test covers only the tools that use
// those. Not covered here (see TestBigtableToolEndpoints, which runs against
// real Bigtable):
//   - bigtable-sql: the emulator returns Unimplemented for PrepareQuery and
//     ExecuteQuery, so GoogleSQL can't run against it.
//   - instance, cluster, logical view and materialized view tools: the
//     emulator has no InstanceAdmin service.
//   - bigtable-list-schemas: it also lists logical and materialized views.
//   - IAM and authentication: the emulator accepts every request.
const (
	bigtableEmulatorImage = "gcr.io/google.com/cloudsdktool/google-cloud-cli:592.0.0-emulators"
	bigtableEmulatorPort  = "8086"

	// The emulator accepts any project and instance.
	bigtableEmulatorProject  = "emulator-project"
	bigtableEmulatorInstance = "emulator-instance"
)

// setupBigtableEmulatorContainer starts the Bigtable emulator and returns its
// host:port. The container is terminated by a t.Cleanup.
func setupBigtableEmulatorContainer(ctx context.Context, t *testing.T) string {
	t.Helper()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        bigtableEmulatorImage,
			Cmd:          []string{"gcloud", "beta", "emulators", "bigtable", "start", "--host-port=0.0.0.0:" + bigtableEmulatorPort},
			ExposedPorts: []string{bigtableEmulatorPort + "/tcp"},
			WaitingFor: wait.ForAll(
				wait.ForLog("Cloud Bigtable emulator running"),
				wait.ForListeningPort(bigtableEmulatorPort+"/tcp"),
			).WithDeadline(5 * time.Minute),
		},
		Started: true,
	})
	// Register termination before checking the error: GenericContainer can
	// return a created container together with a start error.
	t.Cleanup(func() {
		if container == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := container.Terminate(cleanupCtx); err != nil {
			t.Errorf("failed to terminate Bigtable emulator container: %s", err)
		}
	})
	if err != nil {
		t.Fatalf("failed to start Bigtable emulator container: %s", err)
	}

	endpoint, err := container.PortEndpoint(ctx, bigtableEmulatorPort+"/tcp", "")
	if err != nil {
		t.Fatalf("failed to get Bigtable emulator endpoint: %s", err)
	}
	return endpoint
}

func TestBigtableEmulatorToolEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	endpoint := setupBigtableEmulatorContainer(ctx, t)

	// The Bigtable client libraries, both here and in the in-process toolbox
	// server, dial BIGTABLE_EMULATOR_HOST without credentials when it is set.
	// t.Setenv restores the previous value when the test ends.
	t.Setenv("BIGTABLE_EMULATOR_HOST", endpoint)

	adminClient, err := bigtable.NewAdminClient(ctx, bigtableEmulatorProject, bigtableEmulatorInstance)
	if err != nil {
		t.Fatalf("failed to create emulator AdminClient: %v", err)
	}
	t.Cleanup(func() { adminClient.Close() })

	// Seed a table through the data plane. Nothing is deleted afterwards
	// because the emulator keeps everything in memory and the container is
	// discarded.
	columnFamilyName := "cf"
	seededTable := "emulator_seeded_table"
	muts, rowKeys := getTestData(columnFamilyName)
	setupBtTable(t, adminClient, ctx, bigtableEmulatorProject, bigtableEmulatorInstance, seededTable, columnFamilyName, muts, rowKeys)
	verifyEmulatorSeedData(t, ctx, seededTable, columnFamilyName, rowKeys)

	toolsFile := map[string]any{
		"sources": map[string]any{
			"my-instance": map[string]any{
				"type":     BigtableSourceType,
				"project":  bigtableEmulatorProject,
				"instance": bigtableEmulatorInstance,
			},
		},
		"tools": map[string]any{},
	}
	toolsMap := toolsFile["tools"].(map[string]any)
	for _, toolType := range []string{
		"bigtable-create-table", "bigtable-get-table", "bigtable-list-tables",
		"bigtable-update-table", "bigtable-delete-table",
	} {
		toolsMap[toolType] = map[string]any{
			"type":   toolType,
			"source": "my-instance",
		}
	}

	cmd, cleanup, err := tests.StartCmd(ctx, toolsFile, "--enable-api")
	if err != nil {
		t.Fatalf("command initialization returned an error: %s", err)
	}
	defer cleanup()

	waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer waitCancel()
	out, err := testutils.WaitForString(waitCtx, regexp.MustCompile(`Server ready to serve`), cmd.Out)
	if err != nil {
		t.Logf("toolbox command logs: \n%s", out)
		t.Fatalf("toolbox didn't start successfully: %s", err)
	}

	runBigtableEmulatorTableToolsTest(t, seededTable, columnFamilyName)
}

// verifyEmulatorSeedData reads the seeded rows back so a broken emulator
// setup fails here instead of in a tool assertion.
func verifyEmulatorSeedData(t *testing.T, ctx context.Context, tableName, columnFamilyName string, wantRowKeys []string) {
	t.Helper()
	client, err := bigtable.NewClient(ctx, bigtableEmulatorProject, bigtableEmulatorInstance)
	if err != nil {
		t.Fatalf("failed to create emulator data client: %v", err)
	}
	defer client.Close()

	var gotRowKeys []string
	err = client.Open(tableName).ReadRows(ctx, bigtable.InfiniteRange(""), func(row bigtable.Row) bool {
		if len(row[columnFamilyName]) == 0 {
			t.Errorf("row %q has no cells in column family %q", row.Key(), columnFamilyName)
		}
		gotRowKeys = append(gotRowKeys, row.Key())
		return true
	})
	if err != nil {
		t.Fatalf("failed to read seeded rows from %s: %v", tableName, err)
	}
	want := slices.Clone(wantRowKeys)
	slices.Sort(want)
	if !slices.Equal(gotRowKeys, want) {
		t.Fatalf("seeded row keys = %v, want %v", gotRowKeys, want)
	}
}

// runBigtableEmulatorTableToolsTest runs the table lifecycle tools against the
// emulator through the toolbox MCP endpoint.
func runBigtableEmulatorTableToolsTest(t *testing.T, seededTable, columnFamilyName string) {
	listTables := func(t *testing.T) []string {
		t.Helper()
		resp := assertMCPSuccess(t, "bigtable-list-tables", map[string]any{})
		if len(resp.Result.Content) == 0 {
			t.Fatalf("bigtable-list-tables returned empty content")
		}
		var tables []string
		if err := json.Unmarshal([]byte(resp.Result.Content[0].Text), &tables); err != nil {
			t.Fatalf("failed to parse bigtable-list-tables output: %v, body: %s", err, resp.Result.Content[0].Text)
		}
		return tables
	}

	t.Run("get seeded table", func(t *testing.T) {
		if tables := listTables(t); !slices.Contains(tables, seededTable) {
			t.Fatalf("bigtable-list-tables = %v, want it to contain %q", tables, seededTable)
		}
		resp := assertMCPSuccess(t, "bigtable-get-table", map[string]any{"table_id": seededTable})
		if len(resp.Result.Content) == 0 {
			t.Fatalf("bigtable-get-table returned empty content")
		}
		var info struct {
			Families []string `json:"Families"`
		}
		if err := json.Unmarshal([]byte(resp.Result.Content[0].Text), &info); err != nil {
			t.Fatalf("failed to parse bigtable-get-table output: %v, body: %s", err, resp.Result.Content[0].Text)
		}
		if !slices.Contains(info.Families, columnFamilyName) {
			t.Fatalf("bigtable-get-table families = %v, want it to contain %q", info.Families, columnFamilyName)
		}
	})

	t.Run("table lifecycle", func(t *testing.T) {
		tableName := "emulator_lifecycle_table"
		cf := "cf1"

		createResp := assertMCPSuccess(t, "bigtable-create-table", map[string]any{
			"table_id":      tableName,
			"column_family": cf,
		})
		if len(createResp.Result.Content) == 0 || !strings.Contains(createResp.Result.Content[0].Text, "table created successfully") {
			t.Fatalf("bigtable-create-table unexpected output: %v", createResp.Result.Content)
		}

		getResp := assertMCPSuccess(t, "bigtable-get-table", map[string]any{"table_id": tableName})
		if len(getResp.Result.Content) == 0 || !strings.Contains(getResp.Result.Content[0].Text, cf) {
			t.Fatalf("bigtable-get-table output does not contain column family %q: %v", cf, getResp.Result.Content)
		}

		if tables := listTables(t); !slices.Contains(tables, tableName) {
			t.Fatalf("bigtable-list-tables = %v, want it to contain %q", tables, tableName)
		}

		updateResp := assertMCPSuccess(t, "bigtable-update-table", map[string]any{
			"table_id":              tableName,
			"disable_change_stream": true,
		})
		if len(updateResp.Result.Content) == 0 || !strings.Contains(updateResp.Result.Content[0].Text, "table updated successfully") {
			t.Fatalf("bigtable-update-table unexpected output: %v", updateResp.Result.Content)
		}

		deleteResp := assertMCPSuccess(t, "bigtable-delete-table", map[string]any{"table_id": tableName})
		if len(deleteResp.Result.Content) == 0 || !strings.Contains(deleteResp.Result.Content[0].Text, "table deleted successfully") {
			t.Fatalf("bigtable-delete-table unexpected output: %v", deleteResp.Result.Content)
		}

		if tables := listTables(t); slices.Contains(tables, tableName) {
			t.Fatalf("bigtable-list-tables = %v, want %q to be deleted", tables, tableName)
		}
	})

	t.Run("get missing table", func(t *testing.T) {
		statusCode, resp, err := tests.InvokeMCPTool(t, "bigtable-get-table", map[string]any{"table_id": "does_not_exist"}, map[string]string{})
		if err != nil {
			t.Fatalf("native error executing bigtable-get-table: %s", err)
		}
		if statusCode != http.StatusOK {
			t.Fatalf("bigtable-get-table: expected HTTP status 200, got %d", statusCode)
		}
		if resp.Error != nil {
			t.Fatalf("bigtable-get-table: unexpected JSON-RPC error: %v", resp.Error)
		}
		if !resp.Result.IsError {
			t.Fatalf("bigtable-get-table on a missing table: expected an error result, got %v", resp.Result.Content)
		}
	})
}
