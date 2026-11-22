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

package firestore

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/googleapis/mcp-toolbox/internal/testutils"
	"github.com/googleapis/mcp-toolbox/tests"
	tcfirestore "github.com/testcontainers/testcontainers-go/modules/gcloud/firestore"
)

const (
	// FirestoreEmulatorImage is the Cloud SDK image that ships the Firestore emulator.
	FirestoreEmulatorImage = "gcr.io/google.com/cloudsdktool/google-cloud-cli:593.0.0-emulators"
	firestoreEmulatorHost  = "FIRESTORE_EMULATOR_HOST"
)

// setupFirestoreEmulator starts the Firestore emulator and points the
// Firestore Go client (used by both the test and the in-process Toolbox
// server) at it via FIRESTORE_EMULATOR_HOST. It returns the project ID.
func setupFirestoreEmulator(ctx context.Context, t *testing.T) string {
	t.Helper()

	container, err := tcfirestore.Run(ctx, FirestoreEmulatorImage)
	// Register termination first so a container that started but failed its
	// wait strategy is still removed.
	t.Cleanup(func() {
		if container == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := container.Terminate(cleanupCtx); err != nil {
			t.Errorf("failed to terminate Firestore emulator container: %s", err)
		}
	})
	if err != nil {
		t.Fatalf("failed to start Firestore emulator container: %s", err)
	}

	// t.Setenv restores the previous value after the test, so the real-GCP
	// test in this package is unaffected.
	t.Setenv(firestoreEmulatorHost, container.URI())
	return container.ProjectID()
}

// TestFirestoreEmulatorToolEndpoints runs the document CRUD, query and
// list-collections tools against the Firestore emulator. The Rules tools are
// only covered by TestFirestoreToolEndpoints, since the Firebase Rules API has
// no emulator.
func TestFirestoreEmulatorToolEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	project := setupFirestoreEmulator(ctx, t)

	client, err := initFirestoreConnection(project, "")
	if err != nil {
		t.Fatalf("unable to create Firestore emulator connection: %s", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	testCollectionName := fmt.Sprintf("test_collection_%s", strings.ReplaceAll(uuid.New().String(), "-", ""))
	testSubCollectionName := fmt.Sprintf("test_subcollection_%s", strings.ReplaceAll(uuid.New().String(), "-", ""))
	testDocID1 := fmt.Sprintf("doc_%s", strings.ReplaceAll(uuid.New().String(), "-", ""))
	testDocID2 := fmt.Sprintf("doc_%s", strings.ReplaceAll(uuid.New().String(), "-", ""))
	testDocID3 := fmt.Sprintf("doc_%s", strings.ReplaceAll(uuid.New().String(), "-", ""))

	docPath1 := fmt.Sprintf("%s/%s", testCollectionName, testDocID1)
	docPath2 := fmt.Sprintf("%s/%s", testCollectionName, testDocID2)
	docPath3 := fmt.Sprintf("%s/%s", testCollectionName, testDocID3)

	// The container is discarded afterwards, but cleanup is still scoped to
	// the test collection like the real-GCP test.
	setupFirestoreTestData(t, ctx, client, testCollectionName, testSubCollectionName,
		testDocID1, testDocID2, testDocID3)

	sourceConfig := map[string]any{
		"type":    FirestoreSourceType,
		"project": project,
	}
	toolsFile := getFirestoreToolsConfig(sourceConfig)

	cmd, cleanup, err := tests.StartCmd(ctx, toolsFile, "--enable-api")
	if err != nil {
		t.Fatalf("command initialization returned an error: %s", err)
	}
	defer cleanup()

	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
	defer waitCancel()
	out, err := testutils.WaitForString(waitCtx, regexp.MustCompile(`Server ready to serve`), cmd.Out)
	if err != nil {
		t.Logf("toolbox command logs: \n%s", out)
		t.Fatalf("toolbox didn't start successfully: %s", err)
	}

	runFirestoreToolGetTest(t)
	runFirestoreMCPToolCallMethod(t, docPath1, docPath2)
	runFirestoreGetDocumentsTest(t, docPath1, docPath2)
	runFirestoreQueryCollectionTest(t, testCollectionName, false)
	runFirestoreQueryTest(t, testCollectionName)
	runFirestoreQuerySelectArrayTest(t, testCollectionName)
	runFirestoreListCollectionsTest(t, testCollectionName, testSubCollectionName, docPath1)
	runFirestoreAddDocumentsTest(t, testCollectionName)
	runFirestoreUpdateDocumentTest(t, testCollectionName, testDocID1)
	runFirestoreDeleteDocumentsTest(t, docPath3)
	runFirestoreRulesUnavailableOnEmulatorTest(t)
}

// runFirestoreRulesUnavailableOnEmulatorTest checks that the source starts
// without a Firebase Rules client in emulator mode and that the Rules tool
// reports that clearly instead of failing at startup.
func runFirestoreRulesUnavailableOnEmulatorTest(t *testing.T) {
	t.Run("get rules on emulator", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1:5000/api/tool/firestore-get-rules/invoke", strings.NewReader(`{}`))
		if err != nil {
			t.Fatalf("unable to create request: %s", err)
		}
		req.Header.Add("Content-type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("unable to send request: %s", err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("unable to read response body: %s", err)
		}
		want := "Firebase Rules API is not available"
		if !strings.Contains(string(body), want) {
			t.Fatalf("response (status %d) does not contain %q: %s", resp.StatusCode, want, body)
		}
	})
}
