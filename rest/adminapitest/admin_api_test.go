//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package adminapitest

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
)

// Reproduces CBG-1412 - JSON strings in some responses not being correctly escaped
func TestPutDocSpecialChar(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()
	testCases := []struct {
		name         string
		pathDocID    string
		method       string
		body         string
		expectedResp int
		eeOnly       bool
	}{
		{
			name:         "Double quote PUT",
			pathDocID:    `doc"55"`,
			method:       "PUT",
			body:         "{}",
			expectedResp: http.StatusCreated,
			eeOnly:       false,
		},
		{
			name:         "Double quote PUT for replicator2",
			pathDocID:    `doc"77"?replicator2=true`,
			method:       "PUT",
			body:         "{}",
			expectedResp: http.StatusCreated,
			eeOnly:       true,
		},
		{
			name:         "Local double quote PUT",
			pathDocID:    `_local/doc"57"`,
			method:       "PUT",
			body:         "{}",
			expectedResp: http.StatusCreated,
			eeOnly:       false,
		},
		{
			name:         "Double quote PUT with attachment",
			pathDocID:    `doc"59"/attachMe`,
			method:       "PUT",
			body:         "{}",
			expectedResp: http.StatusCreated, // Admin Docs expected response http.StatusOK
			eeOnly:       false,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.eeOnly && !base.IsEnterpriseEdition() {
				t.Skipf("Skipping enterprise-only test")
			}
			tr := rt.SendAdminRequest(testCase.method, fmt.Sprintf("/db/%s", testCase.pathDocID), testCase.body)
			rest.RequireStatus(t, tr, testCase.expectedResp)
			var body map[string]interface{}
			err := json.Unmarshal(tr.BodyBytes(), &body)
			assert.NoError(t, err)
		})
	}

	t.Run("Delete Double quote Doc ID", func(t *testing.T) { // Should be done for Local Document deletion when it returns response
		tr := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s", `del"ete"Me`), "{}") // Create the doc to delete
		rest.RequireStatus(t, tr, http.StatusCreated)
		var putBody struct {
			Rev string `json:"rev"`
		}
		err := json.Unmarshal(tr.BodyBytes(), &putBody)
		assert.NoError(t, err)

		tr = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/%s?rev=%s", `del"ete"Me`, putBody.Rev), "{}")
		rest.RequireStatus(t, tr, http.StatusOK)
		var body map[string]interface{}
		err = json.Unmarshal(tr.BodyBytes(), &body)
		assert.NoError(t, err)
	})
}

// Reproduces #3048 Panic when attempting to make invalid update to a conflicting document
func TestNoPanicInvalidUpdate(t *testing.T) {

	var rt = rest.NewRestTester(t, nil)
	defer rt.Close()

	docId := "conflictTest"

	// Create doc
	response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s", docId), `{"value":"initial"}`)
	response.DumpBody()

	rest.RequireStatus(t, response, http.StatusCreated)

	// Discover revision ID
	// TODO: The schema for SG responses should be defined in our code somewhere to avoid this clunky approach
	var responseDoc map[string]interface{}
	if err := base.JSONUnmarshal(response.Body.Bytes(), &responseDoc); err != nil {
		t.Fatalf("Error unmarshalling response: %v", err)
	}
	revId := responseDoc["rev"].(string)
	revGeneration, revIdHash := db.ParseRevID(revId)
	assert.Equal(t, 1, revGeneration)

	// Update doc (normal update, no conflicting revisions added)
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s", docId), fmt.Sprintf(`{"value":"secondval", db.BodyRev:"%s"}`, revId))
	response.DumpBody()

	// Create conflict
	input := fmt.Sprintf(`
                  {"value": "conflictval",
                   "_revisions": {"start": 2, "ids": ["conflicting_rev", "%s"]}}`, revIdHash)

	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s?new_edits=false", docId), input)
	response.DumpBody()
	if err := base.JSONUnmarshal(response.Body.Bytes(), &responseDoc); err != nil {
		t.Fatalf("Error unmarshalling response: %v", err)
	}
	revId = responseDoc["rev"].(string)
	revGeneration, _ = db.ParseRevID(revId)
	assert.Equal(t, 2, revGeneration)

	// Create conflict again, should be a no-op and return the same response as previous attempt
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s?new_edits=false", docId), input)
	response.DumpBody()
	if err := base.JSONUnmarshal(response.Body.Bytes(), &responseDoc); err != nil {
		t.Fatalf("Error unmarshalling response: %v", err)
	}
	revId = responseDoc["rev"].(string)
	revGeneration, _ = db.ParseRevID(revId)
	assert.Equal(t, 2, revGeneration)

}

func TestUserPasswordValidation(t *testing.T) {

	// PUT a user
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "password":"letmein", "admin_channels":["foo", "bar"]}`)
	rest.RequireStatus(t, response, 201)

	// PUT a user without a password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/ajresnopassword", `{"email":"ajres@couchbase.com", "admin_channels":["foo", "bar"]}`)
	rest.RequireStatus(t, response, 400)

	// POST a user without a password, should fail
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"ajresnopassword", "email":"ajres@couchbase.com", "admin_channels":["foo", "bar"]}`)
	rest.RequireStatus(t, response, 400)

	// PUT a user with a two character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/ajresnopassword", `{"email":"ajres@couchbase.com", "password":"in", "admin_channels":["foo", "bar"]}`)
	rest.RequireStatus(t, response, 400)

	// POST a user with a two character password, should fail
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"ajresnopassword", "email":"ajres@couchbase.com", "password":"an", "admin_channels":["foo", "bar"]}`)
	rest.RequireStatus(t, response, 400)

	// PUT a user with a zero character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/ajresnopassword", `{"email":"ajres@couchbase.com", "password":"", "admin_channels":["foo", "bar"]}`)
	rest.RequireStatus(t, response, 400)

	// POST a user with a zero character password, should fail
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"ajresnopassword", "email":"ajres@couchbase.com", "password":"", "admin_channels":["foo", "bar"]}`)
	rest.RequireStatus(t, response, 400)

	// PUT update a user with a two character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"an"}`)
	rest.RequireStatus(t, response, 400)

	// PUT update a user with a one character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"a"}`)
	rest.RequireStatus(t, response, 400)

	// PUT update a user with a zero character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":""}`)
	rest.RequireStatus(t, response, 400)

	// PUT update a user with a three character password, should succeed
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"abc"}`)
	rest.RequireStatus(t, response, 200)
}

func TestUserAllowEmptyPassword(t *testing.T) {

	// PUT a user
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{AllowEmptyPassword: base.BoolPtr(true)}}})
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "password":"letmein", "admin_channels":["foo", "bar"]}`)
	rest.RequireStatus(t, response, 201)

	// PUT a user without a password, should succeed
	response = rt.SendAdminRequest("PUT", "/db/_user/nopassword1", `{"email":"ajres@couchbase.com", "admin_channels":["foo", "bar"]}`)
	rest.RequireStatus(t, response, 201)

	// POST a user without a password, should succeed
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"nopassword2", "email":"ajres@couchbase.com", "admin_channels":["foo", "bar"]}`)
	rest.RequireStatus(t, response, 201)

	// PUT a user with a two character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/nopassword3", `{"email":"ajres@couchbase.com", "password":"in", "admin_channels":["foo", "bar"]}`)
	rest.RequireStatus(t, response, 400)

	// POST a user with a two character password, should fail
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"nopassword4", "email":"ajres@couchbase.com", "password":"an", "admin_channels":["foo", "bar"]}`)
	rest.RequireStatus(t, response, 400)

	// PUT a user with a zero character password, should succeed
	response = rt.SendAdminRequest("PUT", "/db/_user/nopassword5", `{"email":"ajres@couchbase.com", "password":"", "admin_channels":["foo", "bar"]}`)
	rest.RequireStatus(t, response, 201)

	// POST a user with a zero character password, should succeed
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"nopassword6", "email":"ajres@couchbase.com", "password":"", "admin_channels":["foo", "bar"]}`)
	rest.RequireStatus(t, response, 201)

	// PUT update a user with a two character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"an"}`)
	rest.RequireStatus(t, response, 400)

	// PUT update a user with a one character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"a"}`)
	rest.RequireStatus(t, response, 400)

	// PUT update a user with a zero character password, should succeed
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":""}`)
	rest.RequireStatus(t, response, 200)

	// PUT update a user with a three character password, should succeed
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"abc"}`)
	rest.RequireStatus(t, response, 200)
}

func TestPrincipalForbidUpdatingChannels(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{})
	defer rt.Close()

	// Users
	// PUT admin_channels
	response := rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "password":"letmein", "admin_channels":["foo", "bar"]}`)
	rest.RequireStatus(t, response, 201)

	// PUT all_channels - should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "all_channels":["baz"]}`)
	rest.RequireStatus(t, response, 400)

	// PUT admin_roles
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "password":"letmein", "admin_roles":["foo", "bar"]}`)
	rest.RequireStatus(t, response, 200)

	// PUT roles - should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "roles":["baz"]}`)
	rest.RequireStatus(t, response, 400)

	// Roles
	// PUT admin_channels
	response = rt.SendAdminRequest("PUT", "/db/_role/test", `{"admin_channels":["foo", "bar"]}`)
	rest.RequireStatus(t, response, 201)

	// PUT all_channels - should fail
	response = rt.SendAdminRequest("PUT", "/db/_role/test", `{"all_channels":["baz"]}`)
	rest.RequireStatus(t, response, 400)
}

// Test user access grant while that user has an active changes feed.  (see issue #880)
func TestUserAccessRace(t *testing.T) {

	base.LongRunningTest(t)

	// This test only runs against Walrus due to known sporadic failures.
	// See https://github.com/couchbase/sync_gateway/issues/3006
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip this test under integration testing")
	}

	syncFunction := `
function(doc, oldDoc) {
  if (doc.type == "list") {
    channel("list-"+doc._id);
  } else if (doc.type == "profile") {
    channel("profiles");
    var user = doc._id.substring(doc._id.indexOf(":")+1);
    if (user !== doc.user_id) {
      throw({forbidden : "profile user_id must match docid"})
    }
    requireUser(user);
    access(user, "profiles");
    channel('profile-'+user);
  } else if (doc.type == "Want") {
    var parts = doc._id.split("-");
    var user = parts[1];
    var i = parts[2];
    requireUser(user);
    channel('profile-'+user);
    access(user, 'list-'+i);
  }
}

`
	rtConfig := rest.RestTesterConfig{SyncFn: syncFunction}
	var rt = rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/_user/bernard", `{"name":"bernard", "password":"letmein", "admin_channels":["profile-bernard"]}`)
	rest.RequireStatus(t, response, 201)

	// Try to force channel initialisation for user bernard
	response = rt.SendAdminRequest("GET", "/db/_user/bernard", "")
	rest.RequireStatus(t, response, 200)

	// Create list docs
	input := `{"docs": [`

	for i := 1; i <= 100; i++ {
		if i > 1 {
			input = input + `,`
		}
		docId := fmt.Sprintf("l_%d", i)
		input = input + fmt.Sprintf(`{"_id":"%s", "type":"list"}`, docId)
	}
	input = input + `]}`
	response = rt.SendAdminRequest("POST", "/db/_bulk_docs", input)

	// Start changes feed
	var wg sync.WaitGroup

	// Init the public handler, to avoid data race initializing in the two usages below.
	_ = rt.SendRequest("GET", "/db", "")

	wg.Add(1)

	numExpectedChanges := 201

	go func() {
		defer wg.Done()

		since := ""

		maxTries := 10
		numTries := 0

		changesAccumulated := []db.ChangeEntry{}

		for {

			// Timeout allows us to read continuous changes after processing is complete.  Needs to be long enough to
			// ensure it doesn't terminate before the first change is sent.
			log.Printf("Invoking _changes?feed=continuous&since=%s&timeout=2000", since)
			changesResponse := rt.Send(rest.RequestByUser("GET", fmt.Sprintf("/db/_changes?feed=continuous&since=%s&timeout=2000", since), "", "bernard"))

			changes, err := readContinuousChanges(changesResponse)
			assert.NoError(t, err)

			changesAccumulated = append(changesAccumulated, changes...)

			if len(changesAccumulated) >= numExpectedChanges {
				log.Printf("Got numExpectedChanges (%d).  Done", numExpectedChanges)
				break
			} else {
				log.Printf("Only received %d out of %d expected changes.  Attempt %d / %d.", len(changesAccumulated), numExpectedChanges, numTries, maxTries)
			}

			// Advance the since value if we got any changes
			if len(changes) > 0 {
				since = changes[len(changes)-1].Seq.String()
				log.Printf("Setting since value to: %s.", since)
			}

			numTries++
			if numTries > maxTries {
				t.Errorf("Giving up trying to receive %d changes.  Only received %d", numExpectedChanges, len(changesAccumulated))
				return
			}

		}

	}()

	// Make bulk docs calls, 100 docs each, all triggering access grants to the list docs.
	for j := 0; j < 1; j++ {

		input := `{"docs": [`
		for i := 1; i <= 100; i++ {
			if i > 1 {
				input = input + `,`
			}
			k := j*100 + i
			docId := fmt.Sprintf("Want-bernard-l_%d", k)
			input = input + fmt.Sprintf(`{"_id":"%s", "type":"Want", "owner":"bernard"}`, docId)
		}
		input = input + `]}`

		log.Printf("Sending 2nd round of _bulk_docs")
		response = rt.Send(rest.RequestByUser("POST", "/db/_bulk_docs", input, "bernard"))
		log.Printf("Sent 2nd round of _bulk_docs")

	}

	// wait for changes feed to complete (time out)
	wg.Wait()
}

func TestLoggingKeys(t *testing.T) {
	if base.GlobalTestLoggingSet.IsTrue() {
		t.Skip("Test does not work when a global test log level is set")
	}

	// Reset logging to initial state, in case any other tests forgot to clean up after themselves
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyNone)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Assert default log channels are enabled
	response := rt.SendAdminRequest("GET", "/_logging", "")
	var logKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &logKeys))
	assert.Equal(t, map[string]interface{}{}, logKeys)

	// Set logKeys, Changes+ should enable Changes (PUT replaces any existing log keys)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging", `{"Changes+":true, "Cache":true, "HTTP":true}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var updatedLogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &updatedLogKeys))
	assert.Equal(t, map[string]interface{}{"Changes": true, "Cache": true, "HTTP": true}, updatedLogKeys)

	// Disable Changes logKey which should also disable Changes+
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes":false}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var deletedLogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &deletedLogKeys))
	assert.Equal(t, map[string]interface{}{"Cache": true, "HTTP": true}, deletedLogKeys)

	// Enable Changes++, which should enable Changes (POST append logKeys)
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes++":true}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var appendedLogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &appendedLogKeys))
	assert.Equal(t, map[string]interface{}{"Changes": true, "Cache": true, "HTTP": true}, appendedLogKeys)

	// Disable Changes++ (POST modifies logKeys)
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes++":false}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var disabledLogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &disabledLogKeys))
	assert.Equal(t, map[string]interface{}{"Cache": true, "HTTP": true}, disabledLogKeys)

	// Re-Enable Changes++, which should enable Changes (POST append logKeys)
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes++":true}`), 200)

	// Disable Changes+ which should disable Changes (POST modifies logKeys)
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes+":false}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var disabled2LogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &disabled2LogKeys))
	assert.Equal(t, map[string]interface{}{"Cache": true, "HTTP": true}, disabled2LogKeys)

	// Re-Enable Changes++, which should enable Changes (POST append logKeys)
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes++":true}`), 200)

	// Disable Changes (POST modifies logKeys)
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/_logging", `{"Changes":false}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var disabled3LogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &disabled3LogKeys))
	assert.Equal(t, map[string]interface{}{"Cache": true, "HTTP": true}, disabled3LogKeys)

	// Disable all logKeys by using PUT with an empty channel list
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging", `{}`), 200)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	var noLogKeys map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &noLogKeys))
	assert.Equal(t, map[string]interface{}{}, noLogKeys)
}

func TestLoggingLevels(t *testing.T) {
	if base.GlobalTestLoggingSet.IsTrue() {
		t.Skip("Test does not work when a global test log level is set")
	}

	// Reset logging to initial state, in case any other tests forgot to clean up after themselves
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyNone)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Log keys should be blank
	response := rt.SendAdminRequest("GET", "/_logging", "")
	var logKeys map[string]bool
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &logKeys))
	assert.Equal(t, map[string]bool{}, logKeys)

	// Set log level via logLevel query parameter
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?logLevel=error", ``), http.StatusOK)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?logLevel=invalidLogLevel", ``), http.StatusBadRequest)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?logLevel=", ``), http.StatusBadRequest)

	// Set log level via old level query parameter
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=1", ``), http.StatusOK)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=2", ``), http.StatusOK)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=3", ``), http.StatusOK)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=10", ``), http.StatusOK) // Value is clamped to acceptable range, without returning an error

	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=0", ``), http.StatusBadRequest) // Zero-value is ignored and body is to be parsed
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=0", `{}`), http.StatusOK)       // Zero-value is ignored and body is to be parsed

	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=invalidLogLevel", ``), http.StatusBadRequest)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?level=", ``), http.StatusBadRequest)

	// Trying to set log level via the body will not work (the endpoint expects a log key map)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging", `{"logLevel": "debug"}`), http.StatusBadRequest)
}

func TestLoggingCombined(t *testing.T) {
	if base.GlobalTestLoggingSet.IsTrue() {
		t.Skip("Test does not work when a global test log level is set")
	}

	// Reset logging to initial state, in case any other tests forgot to clean up after themselves
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyNone)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Log keys should be blank
	response := rt.SendAdminRequest("GET", "/_logging", "")
	var logKeys map[string]bool
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &logKeys))
	assert.Equal(t, map[string]bool{}, logKeys)

	// Set log keys and log level in a single request
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/_logging?logLevel=trace", `{"Changes":true, "Cache":true, "HTTP":true}`), http.StatusOK)

	response = rt.SendAdminRequest("GET", "/_logging", "")
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &logKeys))
	assert.Equal(t, map[string]bool{"Changes": true, "Cache": true, "HTTP": true}, logKeys)
}

func TestGetStatus(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendRequest("GET", "/_status", "")
	rest.RequireStatus(t, response, 404)

	response = rt.SendAdminRequest("GET", "/_status", "")
	rest.RequireStatus(t, response, 200)
	var responseBody rest.Status
	err := base.JSONUnmarshal(response.Body.Bytes(), &responseBody)
	assert.NoError(t, err)

	assert.Equal(t, base.LongVersionString, responseBody.Version)

	response = rt.SendAdminRequest("OPTIONS", "/_status", "")
	rest.RequireStatus(t, response, 204)
	assert.Equal(t, "GET", response.Header().Get("Allow"))
}

// Test user delete while that user has an active changes feed (see issue 809)
func TestUserDeleteDuringChangesWithAccess(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache, base.KeyHTTP)

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel); if(doc.type == "setaccess") { access(doc.owner, doc.channel);}}`}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/_user/bernard", `{"name":"bernard", "password":"letmein", "admin_channels":["foo"]}`)
	rest.RequireStatus(t, response, 201)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		changesResponse := rt.Send(rest.RequestByUser("GET", "/db/_changes?feed=continuous&since=0&timeout=3000", "", "bernard"))
		// When testing single threaded, this reproduces the issue described in #809.
		// When testing multithreaded (-cpu 4 -race), there are three (valid) possibilities"
		// 1. The DELETE gets processed before the _changes auth completes: this will return 401
		// 2. The _changes request gets processed before the DELETE: the changes response will be closed when the user is deleted
		// 3. The DELETE is processed after the _changes auth completes, but before the MultiChangesFeed is instantiated.  The
		//  changes feed doesn't have a trigger to attempt a reload of the user in this scenario, so will continue until disconnected
		//  by the client.  This should be fixed more generally (to terminate all active user sessions when the user is deleted, not just
		//  changes feeds) but that enhancement is too high risk to introduce at this time.  The timeout on changes will terminate the unit
		//  test.
		if changesResponse.Code == 401 {
			// case 1 - ok
		} else {
			// case 2 - ensure no error processing the changes response.  The number of entries may vary, depending
			// on whether the changes loop performed an additional iteration before catching the deleted user.
			_, err := readContinuousChanges(changesResponse)
			assert.NoError(t, err)
		}
	}()

	// TODO: sleep required to ensure the changes feed iteration starts before the delete gets processed.
	time.Sleep(500 * time.Millisecond)
	rt.SendAdminRequest("PUT", "/db/bernard_doc1", `{"type":"setaccess", "owner":"bernard","channel":"foo"}`)
	rt.SendAdminRequest("DELETE", "/db/_user/bernard", "")
	rt.SendAdminRequest("PUT", "/db/manny_doc1", `{"type":"setaccess", "owner":"manny","channel":"bar"}`)
	rt.SendAdminRequest("PUT", "/db/bernard_doc2", `{"type":"general", "channel":"foo"}`)

	// case 3
	for i := 0; i <= 5; i++ {
		docId := fmt.Sprintf("/db/bernard_doc%d", i+3)
		response = rt.SendAdminRequest("PUT", docId, `{"type":"setaccess", "owner":"bernard", "channel":"foo"}`)
	}

	wg.Wait()
}

// Reads continuous changes feed response into slice of ChangeEntry
func readContinuousChanges(response *rest.TestResponse) ([]db.ChangeEntry, error) {
	var change db.ChangeEntry
	changes := make([]db.ChangeEntry, 0)
	reader := bufio.NewReader(response.Body)
	for {
		entry, readError := reader.ReadBytes('\n')
		if readError == io.EOF {
			// done
			break
		}
		if readError != nil {
			// unexpected read error
			return changes, readError
		}
		entry = bytes.TrimSpace(entry)
		if len(entry) > 0 {
			err := base.JSONUnmarshal(entry, &change)
			if err != nil {
				return changes, err
			}
			changes = append(changes, change)
			log.Printf("Got change ==> %v", change)
		}

	}
	return changes, nil
}

func TestRoleAPI(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// PUT a role
	rest.RequireStatus(t, rt.SendAdminRequest("GET", "/db/_role/hipster", ""), 404)
	response := rt.SendAdminRequest("PUT", "/db/_role/hipster", `{"admin_channels":["fedoras", "fixies"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/_role/testdeleted", `{"admin_channels":["fedoras", "fixies"]}`)
	rest.RequireStatus(t, response, 201)
	rest.RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_role/testdeleted", ""), 200)

	// GET the role and make sure the result is OK
	response = rt.SendAdminRequest("GET", "/db/_role/hipster", "")
	rest.RequireStatus(t, response, 200)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "hipster", body["name"])
	assert.Equal(t, []interface{}{"fedoras", "fixies"}, body["admin_channels"])
	assert.Equal(t, nil, body["password"])

	response = rt.SendAdminRequest("GET", "/db/_role/", "")
	rest.RequireStatus(t, response, 200)
	assert.Equal(t, `["hipster"]`, response.Body.String())

	// DELETE the role
	rest.RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_role/hipster", ""), 200)
	rest.RequireStatus(t, rt.SendAdminRequest("GET", "/db/_role/hipster", ""), 404)

	// POST a role
	response = rt.SendAdminRequest("POST", "/db/_role", `{"name":"hipster", "admin_channels":["fedoras", "fixies"]}`)
	rest.RequireStatus(t, response, 301)
	response = rt.SendAdminRequest("POST", "/db/_role/", `{"name":"hipster", "admin_channels":["fedoras", "fixies"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_role/hipster", "")
	rest.RequireStatus(t, response, 200)
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "hipster", body["name"])
	rest.RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_role/hipster", ""), 200)

	// GET including deleted
	response = rt.SendAdminRequest("GET", "/db/_role/?deleted=true", "")
	rest.RequireStatus(t, response, 200)
	assert.Equal(t, `["hipster","testdeleted"]`, response.Body.String())
}

func TestGuestUser(t *testing.T) {

	guestUserEndpoint := fmt.Sprintf("/db/_user/%s", base.GuestUsername)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest(http.MethodGet, guestUserEndpoint, "")
	rest.RequireStatus(t, response, http.StatusOK)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, base.GuestUsername, body["name"])
	// This ain't no admin-party, this ain't no nightclub, this ain't no fooling around:
	assert.Nil(t, body["admin_channels"])

	// Disable the guest user:
	response = rt.SendAdminRequest(http.MethodPut, guestUserEndpoint, `{"disabled":true}`)
	rest.RequireStatus(t, response, http.StatusOK)

	// Get guest user and verify it is now disabled:
	response = rt.SendAdminRequest(http.MethodGet, guestUserEndpoint, "")
	rest.RequireStatus(t, response, http.StatusOK)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, base.GuestUsername, body["name"])
	assert.True(t, body["disabled"].(bool))

	// Check that the actual User object is correct:
	ctx := rt.Context()
	user, _ := rt.ServerContext().Database(ctx, "db").Authenticator(ctx).GetUser("")
	assert.Empty(t, user.Name())
	assert.Nil(t, user.ExplicitChannels())
	assert.True(t, user.Disabled())

	// We can't delete the guest user, but we should get a reasonable error back.
	response = rt.SendAdminRequest(http.MethodDelete, guestUserEndpoint, "")
	rest.RequireStatus(t, response, http.StatusMethodNotAllowed)
}

// Test that TTL values greater than the default max offset TTL 2592000 seconds are processed correctly
// fixes #974
func TestSessionTtlGreaterThan30Days(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	a := auth.NewAuthenticator(rt.Bucket(), nil, auth.DefaultAuthenticatorOptions())
	user, err := a.GetUser("")
	assert.NoError(t, err)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.NoError(t, err)

	user, err = a.GetUser("")
	assert.NoError(t, err)
	assert.True(t, user.Disabled())

	response := rt.SendRequest("PUT", "/db/doc", `{"hi": "there"}`)
	rest.RequireStatus(t, response, 401)

	user, err = a.NewUser("pupshaw", "letmein", channels.BaseSetOf(t, "*"))
	assert.NoError(t, a.Save(user))

	// create a session with the maximum offset ttl value (30days) 2592000 seconds
	response = rt.SendAdminRequest("POST", "/db/_session", `{"name":"pupshaw", "ttl":2592000}`)
	rest.RequireStatus(t, response, 200)

	layout := "2006-01-02T15:04:05"

	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))

	log.Printf("expires %s", body["expires"].(string))
	expires, err := time.Parse(layout, body["expires"].(string)[:19])
	assert.NoError(t, err)

	// create a session with a ttl value one second greater thatn the max offset ttl 2592001 seconds
	response = rt.SendAdminRequest("POST", "/db/_session", `{"name":"pupshaw", "ttl":2592001}`)
	rest.RequireStatus(t, response, 200)

	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	log.Printf("expires2 %s", body["expires"].(string))
	expires2, err := time.Parse(layout, body["expires"].(string)[:19])
	assert.NoError(t, err)

	// Allow a ten second drift between the expires dates, to pass test on slow servers
	acceptableTimeDelta := time.Duration(10) * time.Second

	// The difference between the two expires dates should be less than the acceptable time delta
	assert.True(t, expires2.Sub(expires) < acceptableTimeDelta)
}

// Check whether the session is getting extended or refreshed if 10% or more of the current
// expiration time has elapsed.
func TestSessionExtension(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{GuestEnabled: true})
	defer rt.Close()

	id, err := base.GenerateRandomSecret()
	require.NoError(t, err)

	// Fake session with more than 10% of the 24 hours TTL has elapsed. It should cause a new
	// cookie to be sent by the server with the same session ID and an extended expiration date.
	fakeSession := auth.LoginSession{
		ID:         id,
		Username:   "Alice",
		Expiration: time.Now().Add(4 * time.Hour),
		Ttl:        24 * time.Hour,
	}

	assert.NoError(t, rt.Bucket().Set(auth.DocIDForSession(fakeSession.ID), 0, nil, fakeSession))
	reqHeaders := map[string]string{
		"Cookie": auth.DefaultCookieName + "=" + fakeSession.ID,
	}

	response := rt.SendRequestWithHeaders("PUT", "/db/doc1", `{"hi": "there"}`, reqHeaders)
	log.Printf("PUT Request: Set-Cookie: %v", response.Header().Get("Set-Cookie"))
	rest.RequireStatus(t, response, http.StatusCreated)
	assert.Contains(t, response.Header().Get("Set-Cookie"), auth.DefaultCookieName+"="+fakeSession.ID)

	response = rt.SendRequestWithHeaders("GET", "/db/doc1", "", reqHeaders)
	log.Printf("GET Request: Set-Cookie: %v", response.Header().Get("Set-Cookie"))
	rest.RequireStatus(t, response, http.StatusOK)
	assert.Equal(t, "", response.Header().Get("Set-Cookie"))

	// Explicitly delete the fake session doc from the bucket to simulate the test
	// scenario for expired session. In reality, Sync Gateway rely on Couchbase
	// Server to nuke the expired document based on TTL. Couchbase Server periodically
	// removes all items with expiration times that have passed.
	assert.NoError(t, rt.Bucket().Delete(auth.DocIDForSession(fakeSession.ID)))

	response = rt.SendRequestWithHeaders("GET", "/db/doc1", "", reqHeaders)
	log.Printf("GET Request: Set-Cookie: %v", response.Header().Get("Set-Cookie"))
	rest.RequireStatus(t, response, http.StatusUnauthorized)

}

func TestSessionAPI(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// create session test users
	response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"user1", "password":"1234"}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"user2", "password":"1234"}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"user3", "password":"1234"}`)
	rest.RequireStatus(t, response, 201)

	// create multiple sessions for the users
	user1sessions := make([]string, 5)
	user2sessions := make([]string, 5)
	user3sessions := make([]string, 5)

	for i := 0; i < 5; i++ {
		user1sessions[i] = createSession(t, rt, "user1")
		user2sessions[i] = createSession(t, rt, "user2")
		user3sessions[i] = createSession(t, rt, "user3")
	}

	// GET Tests
	// 1. GET a session and make sure the result is OK
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user1sessions[0]), "")
	rest.RequireStatus(t, response, 200)

	// DELETE tests
	// 1. DELETE a session by session id
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/_session/%s", user1sessions[0]), "")
	rest.RequireStatus(t, response, 200)

	// Attempt to GET the deleted session and make sure it's not found
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user1sessions[0]), "")
	rest.RequireStatus(t, response, 404)

	// 2. DELETE a session with user validation
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/_user/%s/_session/%s", "user1", user1sessions[1]), "")
	rest.RequireStatus(t, response, 200)

	// Attempt to GET the deleted session and make sure it's not found
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user1sessions[1]), "")
	rest.RequireStatus(t, response, 404)

	// 3. DELETE a session not belonging to the user (should fail)
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/_user/%s/_session/%s", "user1", user2sessions[0]), "")
	rest.RequireStatus(t, response, 404)

	// GET the session and make sure it still exists
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user2sessions[0]), "")
	rest.RequireStatus(t, response, 200)

	// 4. DELETE all sessions for a user
	response = rt.SendAdminRequest("DELETE", "/db/_user/user2/_session", "")
	rest.RequireStatus(t, response, 200)

	// Validate that all sessions were deleted
	for i := 0; i < 5; i++ {
		response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user2sessions[i]), "")
		rest.RequireStatus(t, response, 404)
	}

	// 5. DELETE sessions when password is changed
	// Change password for user3
	response = rt.SendAdminRequest("PUT", "/db/_user/user3", `{"password":"5678"}`)
	rest.RequireStatus(t, response, 200)

	// Validate that all sessions were deleted
	for i := 0; i < 5; i++ {
		response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user3sessions[i]), "")
		rest.RequireStatus(t, response, 404)
	}

	// DELETE the users
	rest.RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/user1", ""), 200)
	rest.RequireStatus(t, rt.SendAdminRequest("GET", "/db/_user/user1", ""), 404)

	rest.RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/user2", ""), 200)
	rest.RequireStatus(t, rt.SendAdminRequest("GET", "/db/_user/user2", ""), 404)

	rest.RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/user3", ""), 200)
	rest.RequireStatus(t, rt.SendAdminRequest("GET", "/db/_user/user3", ""), 404)

}

func TestFlush(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("sgbucket.DeleteableBucket inteface only supported by Walrus")
	}

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateDoc(t, "doc1")
	rt.CreateDoc(t, "doc2")
	rest.RequireStatus(t, rt.SendAdminRequest("GET", "/db/doc1", ""), 200)
	rest.RequireStatus(t, rt.SendAdminRequest("GET", "/db/doc2", ""), 200)

	log.Printf("Flushing db...")
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/db/_flush", ""), 200)
	require.NoError(t, rt.SetAdminParty(true)) // needs to be re-enabled after flush since guest user got wiped

	// After the flush, the db exists but the documents are gone:
	rest.RequireStatus(t, rt.SendAdminRequest("GET", "/db/", ""), 200)
	rest.RequireStatus(t, rt.SendAdminRequest("GET", "/db/doc1", ""), 404)
	rest.RequireStatus(t, rt.SendAdminRequest("GET", "/db/doc2", ""), 404)
}

// Test a single call to take DB offline
func TestDBOfflineSingle(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)
	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")
}

// Make two concurrent calls to take DB offline
// Ensure both calls succeed and that DB is offline
// when both calls return
func TestDBOfflineConcurrent(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	// Take DB offline concurrently using two goroutines
	// Both should return success and DB should be offline
	// once both goroutines return
	var wg sync.WaitGroup
	wg.Add(2)

	var goroutineresponse1 *rest.TestResponse
	go func() {
		goroutineresponse1 = rt.SendAdminRequest("POST", "/db/_offline", "")
		wg.Done()
	}()

	var goroutineresponse2 *rest.TestResponse
	go func() {
		goroutineresponse2 = rt.SendAdminRequest("POST", "/db/_offline", "")
		wg.Done()
	}()

	err := rest.WaitWithTimeout(&wg, time.Second*30)
	assert.NoError(t, err, "Error waiting for waitgroup")
	rest.RequireStatus(t, goroutineresponse1, http.StatusOK)
	rest.RequireStatus(t, goroutineresponse2, http.StatusOK)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

}

// Test that a DB can be created offline
func TestStartDBOffline(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)
	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")
}

// Take DB offline and ensure that normal REST calls
// fail with status 503
func TestDBOffline503Response(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	rest.RequireStatus(t, rt.SendRequest("GET", "/db/doc1", ""), 503)
}

// Take DB offline and ensure can put db config
func TestDBOfflinePutDbConfig(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	rest.RequireStatus(t, rt.SendRequest("PUT", "/db/_config", ""), 404)
}

// Tests that the users returned in the config endpoint have the correct names
// Reproduces #2223
func TestDBGetConfigNames(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	p := "password"

	rt.DatabaseConfig = &rest.DatabaseConfig{DbConfig: rest.DbConfig{
		Users: map[string]*auth.PrincipalConfig{
			"alice": &auth.PrincipalConfig{Password: &p},
			"bob":   &auth.PrincipalConfig{Password: &p},
		},
	}}

	response := rt.SendAdminRequest("GET", "/db/_config?include_runtime=true", "")
	var body rest.DbConfig
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))

	assert.Equal(t, len(rt.DatabaseConfig.Users), len(body.Users))

	for k, v := range body.Users {
		assert.Equal(t, k, *v.Name)
	}

}

// Take DB offline and ensure can post _resync
func TestDBOfflinePostResync(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/db/_resync?action=start", ""), 200)
	err := rt.WaitForCondition(func() bool {
		response := rt.SendAdminRequest("GET", "/db/_resync", "")
		var status db.ResyncManagerResponse
		err := json.Unmarshal(response.BodyBytes(), &status)
		assert.NoError(t, err)

		var val interface{}
		_, err = rt.Bucket().Get(rt.GetDatabase().ResyncManager.GetHeartbeatDocID(t), &val)

		return status.State == db.BackgroundProcessStateCompleted && base.IsDocNotFoundError(err)
	})
	assert.NoError(t, err)
}

// Take DB offline and ensure only one _resync can be in progress
func TestDBOfflineSingleResync(t *testing.T) {

	syncFn := `
	function(doc) {
		channel("x")
	}`
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SyncFn: syncFn})
	defer rt.Close()

	// create documents in DB to cause resync to take a few seconds
	for i := 0; i < 1000; i++ {
		rt.CreateDoc(t, fmt.Sprintf("doc%v", i))
	}
	assert.Equal(t, int64(1000), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusOK)

	// Send a second _resync request.  This must return a 400 since the first one is blocked processing
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/db/_resync?action=start", ""), 503)

	err := rt.WaitForCondition(func() bool {
		response := rt.SendAdminRequest("GET", "/db/_resync", "")
		var status db.ResyncManagerResponse
		err := json.Unmarshal(response.BodyBytes(), &status)
		assert.NoError(t, err)

		var val interface{}
		_, err = rt.Bucket().Get(rt.GetDatabase().ResyncManager.GetHeartbeatDocID(t), &val)

		return status.State == db.BackgroundProcessStateCompleted && base.IsDocNotFoundError(err)
	})
	assert.NoError(t, err)

	assert.Equal(t, int64(2000), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())
}

func TestResync(t *testing.T) {
	base.LongRunningTest(t)

	testCases := []struct {
		name               string
		docsCreated        int
		expectedSyncFnRuns int
		expectedQueryCount int
		queryLimit         int
	}{
		{
			name:               "Docs 0, Limit Default",
			docsCreated:        0,
			expectedSyncFnRuns: 0,
			expectedQueryCount: 1,
			queryLimit:         db.DefaultQueryPaginationLimit,
		},
		{
			name:               "Docs 1000, Limit Default",
			docsCreated:        1000,
			expectedSyncFnRuns: 2000,
			expectedQueryCount: 1,
			queryLimit:         db.DefaultQueryPaginationLimit,
		},
		{
			name:               "Docs 1000, Limit 10",
			docsCreated:        1000,
			expectedSyncFnRuns: 2000,
			expectedQueryCount: 101,
			queryLimit:         10,
		},
	}

	syncFn := `
	function(doc) {
		channel("x")
	}`

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rt := rest.NewRestTester(t,
				&rest.RestTesterConfig{
					DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
						QueryPaginationLimit: &testCase.queryLimit,
					}},
					SyncFn: syncFn,
				},
			)
			defer rt.Close()

			for i := 0; i < testCase.docsCreated; i++ {
				rt.CreateDoc(t, fmt.Sprintf("doc%d", i))
			}

			response := rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
			rest.RequireStatus(t, response, http.StatusServiceUnavailable)

			response = rt.SendAdminRequest("POST", "/db/_offline", "")
			rest.RequireStatus(t, response, http.StatusOK)

			rest.WaitAndAssertCondition(t, func() bool {
				state := atomic.LoadUint32(&rt.GetDatabase().State)
				return state == db.DBOffline
			})

			response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
			rest.RequireStatus(t, response, http.StatusOK)

			var resyncManagerStatus db.ResyncManagerResponse
			err := rt.WaitForCondition(func() bool {
				response := rt.SendAdminRequest("GET", "/db/_resync", "")
				err := json.Unmarshal(response.BodyBytes(), &resyncManagerStatus)
				assert.NoError(t, err)

				var val interface{}
				_, err = rt.Bucket().Get(rt.GetDatabase().ResyncManager.GetHeartbeatDocID(t), &val)

				if resyncManagerStatus.State == db.BackgroundProcessStateCompleted && base.IsDocNotFoundError(err) {
					return true
				} else {
					t.Logf("resyncManagerStatus.State != %v: %v - err:%v", db.BackgroundProcessStateCompleted, resyncManagerStatus.State, err)
					return false
				}
			})
			assert.NoError(t, err)

			assert.Equal(t, testCase.expectedSyncFnRuns, int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()))

			var queryName string
			if base.TestsDisableGSI() {
				queryName = fmt.Sprintf(base.StatViewFormat, db.DesignDocSyncGateway(), db.ViewChannels)
			} else {
				queryName = db.QueryTypeChannels
			}

			assert.Equal(t, testCase.expectedQueryCount, int(rt.GetDatabase().DbStats.Query(queryName).QueryCount.Value()))
			assert.Equal(t, testCase.docsCreated, resyncManagerStatus.DocsProcessed)
			assert.Equal(t, 0, resyncManagerStatus.DocsChanged)
		})
	}

}

func TestResyncErrorScenarios(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		// Limitation of setting LeakyBucket on RestTester
		t.Skip("This test only works with walrus")
	}

	syncFn := `
	function(doc) {
		channel("x")
	}`

	leakyTestBucket := base.GetTestBucket(t).LeakyBucketClone(base.LeakyBucketConfig{})

	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn:           syncFn,
			CustomTestBucket: leakyTestBucket,
		},
	)
	defer rt.Close()

	leakyBucket, ok := base.AsLeakyBucket(rt.Bucket())
	require.Truef(t, ok, "Wanted *base.LeakyBucket but got %T", leakyTestBucket.Bucket)

	var (
		useCallback   bool
		callbackFired bool
	)

	if base.TestsDisableGSI() {
		leakyBucket.SetPostQueryCallback(func(ddoc, viewName string, params map[string]interface{}) {
			if useCallback {
				callbackFired = true
				response := rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
				rest.RequireStatus(t, response, http.StatusServiceUnavailable)
				useCallback = false
			}
		})
	} else {
		leakyBucket.SetPostN1QLQueryCallback(func() {
			if useCallback {
				callbackFired = true
				response := rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
				rest.RequireStatus(t, response, http.StatusServiceUnavailable)
				useCallback = false
			}
		})
	}

	for i := 0; i < 1000; i++ {
		rt.CreateDoc(t, fmt.Sprintf("doc%d", i))
	}

	response := rt.SendAdminRequest("GET", "/db/_resync", "")
	rest.RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusServiceUnavailable)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	rest.RequireStatus(t, response, http.StatusBadRequest)

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, http.StatusOK)

	rest.WaitAndAssertCondition(t, func() bool {
		state := atomic.LoadUint32(&rt.GetDatabase().State)
		return state == db.DBOffline
	})

	useCallback = true
	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusOK)

	rest.WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateCompleted, rt.GetDatabase().ResyncManager.GetRunState)
	rest.WaitAndAssertBackgroundManagerExpiredHeartbeat(t, rt.GetDatabase().ResyncManager)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
	rest.RequireStatus(t, response, http.StatusBadRequest)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=invalid", "")
	rest.RequireStatus(t, response, http.StatusBadRequest)

	// Test empty action, should default to start
	response = rt.SendAdminRequest("POST", "/db/_resync", "")
	rest.RequireStatus(t, response, http.StatusOK)

	rest.WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateCompleted, rt.GetDatabase().ResyncManager.GetRunState)
	rest.WaitAndAssertBackgroundManagerExpiredHeartbeat(t, rt.GetDatabase().ResyncManager)

	assert.True(t, callbackFired, "expecting callback to be fired")
}

func TestResyncStop(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		// Limitation of setting LeakyBucket on RestTester
		t.Skip("This test only works with walrus")
	}

	syncFn := `
	function(doc) {
		channel("x")
	}`

	leakyTestBucket := base.GetTestBucket(t).LeakyBucketClone(base.LeakyBucketConfig{})

	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: syncFn,
			DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
				QueryPaginationLimit: base.IntPtr(10),
			}},
			CustomTestBucket: leakyTestBucket,
		},
	)
	defer rt.Close()

	leakyBucket, ok := base.AsLeakyBucket(rt.Bucket())
	require.Truef(t, ok, "Wanted *base.LeakyBucket but got %T", leakyTestBucket.Bucket)

	var (
		useCallback   bool
		callbackFired bool
	)

	if base.TestsDisableGSI() {
		leakyBucket.SetPostQueryCallback(func(ddoc, viewName string, params map[string]interface{}) {
			if useCallback {
				callbackFired = true
				response := rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
				rest.RequireStatus(t, response, http.StatusOK)
				useCallback = false
			}
		})
	} else {
		leakyBucket.SetPostN1QLQueryCallback(func() {
			if useCallback {
				callbackFired = true
				response := rt.SendAdminRequest("POST", "/db/_resync?action=stop", "")
				rest.RequireStatus(t, response, http.StatusOK)
				useCallback = false
			}
		})
	}

	for i := 0; i < 1000; i++ {
		rt.CreateDoc(t, fmt.Sprintf("doc%d", i))
	}

	err := rt.WaitForCondition(func() bool {
		return int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value()) == 1000
	})
	assert.NoError(t, err)

	response := rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, http.StatusOK)

	rest.WaitAndAssertCondition(t, func() bool {
		state := atomic.LoadUint32(&rt.GetDatabase().State)
		return state == db.DBOffline
	})

	useCallback = true
	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusOK)

	rest.WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateStopped, rt.GetDatabase().ResyncManager.GetRunState)
	rest.WaitAndAssertBackgroundManagerExpiredHeartbeat(t, rt.GetDatabase().ResyncManager)

	assert.True(t, callbackFired, "expecting callback to be fired")

	syncFnCount := int(rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())
	assert.True(t, syncFnCount < 2000, "Expected syncFnCount < 2000 but syncFnCount=%d", syncFnCount)
}

func TestResyncRegenerateSequences(t *testing.T) {

	base.LongRunningTest(t)
	syncFn := `
	function(doc) {
		if (doc.userdoc){
			channel("channel_1")
		}
	}`

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	var testBucket *base.TestBucket

	if base.UnitTestUrlIsWalrus() {
		var closeFn func()
		testBucket, closeFn = base.GetPersistentWalrusBucket(t)
		defer closeFn()
	} else {
		testBucket = base.GetTestBucket(t)
	}

	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn:           syncFn,
			CustomTestBucket: testBucket,
		},
	)
	defer rt.Close()

	var response *rest.TestResponse
	var docSeqArr []float64
	var body db.Body

	for i := 0; i < 10; i++ {
		docID := fmt.Sprintf("doc%d", i)
		rt.CreateDoc(t, docID)

		response = rt.SendAdminRequest("GET", "/db/_raw/"+docID, "")
		require.Equal(t, http.StatusOK, response.Code)

		err := json.Unmarshal(response.BodyBytes(), &body)
		require.NoError(t, err)

		docSeqArr = append(docSeqArr, body["_sync"].(map[string]interface{})["sequence"].(float64))
	}

	role := "role1"
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_role/%s", role), fmt.Sprintf(`{"name":"%s", "admin_channels":["channel_1"]}`, role))
	rest.RequireStatus(t, response, http.StatusCreated)

	username := "user1"
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_user/%s", username), fmt.Sprintf(`{"name":"%s", "password":"letmein", "admin_channels":["channel_1"], "admin_roles": ["%s"]}`, username, role))
	rest.RequireStatus(t, response, http.StatusCreated)

	_, err := rt.Bucket().Get(base.RolePrefix+"role1", &body)
	assert.NoError(t, err)
	role1SeqBefore := body["sequence"].(float64)

	_, err = rt.Bucket().Get(base.UserPrefix+"user1", &body)
	assert.NoError(t, err)
	user1SeqBefore := body["sequence"].(float64)

	response = rt.SendAdminRequest("PUT", "/db/userdoc", `{"userdoc": true}`)
	rest.RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("PUT", "/db/userdoc2", `{"userdoc": true}`)
	rest.RequireStatus(t, response, http.StatusCreated)

	// Let everything catch up before opening changes feed
	require.NoError(t, rt.WaitForPendingChanges())

	type ChangesResp struct {
		Results []struct {
			ID  string `json:"id"`
			Seq int    `json:"seq"`
		} `json:"results"`
		LastSeq string `json:"last_seq"`
	}

	changesRespContains := func(changesResp ChangesResp, docid string) bool {
		for _, resp := range changesResp.Results {
			if resp.ID == docid {
				return true
			}
		}
		return false
	}

	var changesResp ChangesResp
	request, _ := http.NewRequest("GET", "/db/_changes", nil)
	request.SetBasicAuth("user1", "letmein")
	response = rt.Send(request)
	rest.RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &changesResp)
	assert.Len(t, changesResp.Results, 3)
	assert.True(t, changesRespContains(changesResp, "userdoc"))
	assert.True(t, changesRespContains(changesResp, "userdoc2"))

	response = rt.SendAdminRequest("GET", "/db/_resync", "")
	rest.RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start&regenerate_sequences=true", "")
	rest.RequireStatus(t, response, http.StatusOK)

	rest.WaitAndAssertBackgroundManagerState(t, db.BackgroundProcessStateCompleted, rt.GetDatabase().ResyncManager.GetRunState)
	rest.WaitAndAssertBackgroundManagerExpiredHeartbeat(t, rt.GetDatabase().ResyncManager)

	_, err = rt.Bucket().Get(base.RolePrefix+"role1", &body)
	assert.NoError(t, err)
	role1SeqAfter := body["sequence"].(float64)

	_, err = rt.Bucket().Get(base.UserPrefix+"user1", &body)
	assert.NoError(t, err)
	user1SeqAfter := body["sequence"].(float64)

	assert.True(t, role1SeqAfter > role1SeqBefore)
	assert.True(t, user1SeqAfter > user1SeqBefore)

	for i := 0; i < 10; i++ {
		docID := fmt.Sprintf("doc%d", i)

		doc, err := rt.GetDatabase().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		assert.True(t, float64(doc.Sequence) > docSeqArr[i])
	}

	response = rt.SendAdminRequest("GET", "/db/_resync", "")
	rest.RequireStatus(t, response, http.StatusOK)
	var resyncStatus db.ResyncManagerResponse
	err = base.JSONUnmarshal(response.BodyBytes(), &resyncStatus)
	assert.NoError(t, err)
	assert.Equal(t, 12, resyncStatus.DocsChanged)
	assert.Equal(t, 12, resyncStatus.DocsProcessed)

	response = rt.SendAdminRequest("POST", "/db/_online", "")
	rest.RequireStatus(t, response, http.StatusOK)

	err = rt.WaitForCondition(func() bool {
		state := atomic.LoadUint32(&rt.GetDatabase().State)
		return state == db.DBOnline
	})
	assert.NoError(t, err)

	// Data is wiped from walrus when brought back online
	request, _ = http.NewRequest("GET", "/db/_changes?since="+changesResp.LastSeq, nil)
	request.SetBasicAuth("user1", "letmein")
	response = rt.Send(request)
	rest.RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &changesResp)
	assert.Len(t, changesResp.Results, 3)
	assert.True(t, changesRespContains(changesResp, "userdoc"))
	assert.True(t, changesRespContains(changesResp, "userdoc2"))
}

// Single threaded bring DB online
func TestDBOnlineSingle(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	rt.SendAdminRequest("POST", "/db/_online", "")
	rest.RequireStatus(t, response, 200)

	time.Sleep(500 * time.Millisecond)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")
}

// Take DB online concurrently using two goroutines
// Both should return success and DB should be online
// once both goroutines return
func TestDBOnlineConcurrent(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	var wg sync.WaitGroup
	wg.Add(2)

	var goroutineresponse1 *rest.TestResponse
	go func(rt *rest.RestTester) {
		defer wg.Done()
		goroutineresponse1 = rt.SendAdminRequest("POST", "/db/_online", "")
		rest.RequireStatus(t, goroutineresponse1, 200)
	}(rt)

	var goroutineresponse2 *rest.TestResponse
	go func(rt *rest.RestTester) {
		defer wg.Done()
		goroutineresponse2 = rt.SendAdminRequest("POST", "/db/_online", "")
		rest.RequireStatus(t, goroutineresponse2, 200)
	}(rt)

	// This only waits until both _online requests have been posted
	// They may not have been processed at this point
	wg.Wait()

	// Wait for DB to come online (retry loop)
	errDbOnline := rt.WaitForDBOnline()
	assert.NoError(t, errDbOnline, "Error waiting for db to come online")

}

// Test bring DB online with delay of 1 second
func TestSingleDBOnlineWithDelay(t *testing.T) {

	t.Skip("Use case covered by TestDBOnlineWithTwoDelays, skipping due to slow test")

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	log.Printf("Taking DB offline")
	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	rt.SendAdminRequest("POST", "/db/_online", "{\"delay\":1}")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	// Wait until after the 1 second delay, since the online request explicitly asked for a delay
	time.Sleep(1500 * time.Millisecond)

	// Wait for DB to come online (retry loop)
	errDbOnline := rt.WaitForDBOnline()
	assert.NoError(t, errDbOnline, "Error waiting for db to come online")

}

// Test bring DB online with delay of 2 seconds
// But bring DB online immediately in separate call
// DB should should only be brought online once
// there should be no errors
func TestDBOnlineWithDelayAndImmediate(t *testing.T) {

	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelTrace, base.KeyAll)

	// CBG-1513: This test is prone to panicing when the walrus bucket was closed and still used
	assert.NotPanicsf(t, func() {
		rt := rest.NewRestTester(t, nil)
		defer rt.Close()

		var response *rest.TestResponse
		var errDBState error

		log.Printf("Taking DB offline")
		require.Equal(t, "Online", rt.GetDBState())

		response = rt.SendAdminRequest("POST", "/db/_offline", "")
		rest.RequireStatus(t, response, 200)

		// Bring DB online with delay of two seconds
		response = rt.SendAdminRequest("POST", "/db/_online", "{\"delay\":1}")
		rest.RequireStatus(t, response, 200)

		require.Equal(t, "Offline", rt.GetDBState())

		// Bring DB online immediately
		response = rt.SendAdminRequest("POST", "/db/_online", "")
		rest.RequireStatus(t, response, 200)

		// Wait for DB to come online (retry loop)
		errDBState = rt.WaitForDBOnline()
		assert.NoError(t, errDBState)

		// Wait until after the 1 second delay, since the online request explicitly asked for a delay
		time.Sleep(1500 * time.Millisecond)

		// Wait for DB to come online (retry loop)
		errDBState = rt.WaitForDBOnline()
		assert.NoError(t, errDBState)
	}, "CBG-1513: panicked when the walrus bucket was closed and still used")
}

// Test bring DB online concurrently with delay of 1 second
// and delay of 2 seconds
// BD should should only be brought online once
// there should be no errors
func TestDBOnlineWithTwoDelays(t *testing.T) {

	base.LongRunningTest(t)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("GET", "/db/", "")
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	rt.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	// Bring DB online with delay of one seconds
	rt.SendAdminRequest("POST", "/db/_online", "{\"delay\":1}")
	rest.RequireStatus(t, response, 200)

	// Bring DB online with delay of two seconds
	rt.SendAdminRequest("POST", "/db/_online", "{\"delay\":2}")
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Offline")

	time.Sleep(1500 * time.Millisecond)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")

	time.Sleep(600 * time.Millisecond)

	response = rt.SendAdminRequest("GET", "/db/", "")
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.True(t, body["state"].(string) == "Online")
}

func createSession(t *testing.T, rt *rest.RestTester, username string) string {

	response := rt.SendAdminRequest("POST", "/db/_session", fmt.Sprintf(`{"name":%q}`, username))
	rest.RequireStatus(t, response, 200)

	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	sessionId := body["session_id"].(string)

	return sessionId
}

func TestPurgeWithBadJsonPayload(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/db/_purge", "foo")
	rest.RequireStatus(t, response, 400)
}

func TestPurgeWithNonArrayRevisionList(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/db/_purge", `{"foo":"list"}`)
	rest.RequireStatus(t, response, 200)

	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, map[string]interface{}{"purged": map[string]interface{}{}}, body)
}

func TestPurgeWithEmptyRevisionList(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/db/_purge", `{"foo":[]}`)
	rest.RequireStatus(t, response, 200)

	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, map[string]interface{}{"purged": map[string]interface{}{}}, body)
}

func TestPurgeWithGreaterThanOneRevision(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/db/_purge", `{"foo":["rev1","rev2"]}`)
	rest.RequireStatus(t, response, 200)

	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, map[string]interface{}{"purged": map[string]interface{}{}}, body)
}

func TestPurgeWithNonStarRevision(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("POST", "/db/_purge", `{"foo":["rev1"]}`)
	rest.RequireStatus(t, response, 200)

	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, map[string]interface{}{"purged": map[string]interface{}{}}, body)
}

func TestPurgeWithStarRevision(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)

	response := rt.SendAdminRequest("POST", "/db/_purge", `{"doc1":["*"]}`)
	rest.RequireStatus(t, response, 200)
	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, map[string]interface{}{"purged": map[string]interface{}{"doc1": []interface{}{"*"}}}, body)

	// Create new versions of the doc1 without conflicts
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)
}

func TestPurgeWithMultipleValidDocs(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/db/doc2", `{"moo":"car"}`), 201)

	response := rt.SendAdminRequest("POST", "/db/_purge", `{"doc1":["*"],"doc2":["*"]}`)
	rest.RequireStatus(t, response, 200)

	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, map[string]interface{}{"purged": map[string]interface{}{"doc1": []interface{}{"*"}, "doc2": []interface{}{"*"}}}, body)

	// Create new versions of the docs without conflicts
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/db/doc2", `{"moo":"car"}`), 201)
}

// TestPurgeWithChannelCache will make sure thant upon calling _purge, the channel caches are also cleaned
// This was fixed in #3765, previously channel caches were not cleaned up
func TestPurgeWithChannelCache(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/db/doc1", `{"foo":"bar", "channels": ["abc", "def"]}`), http.StatusCreated)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/db/doc2", `{"moo":"car", "channels": ["abc"]}`), http.StatusCreated)

	changes, err := rt.WaitForChanges(2, "/db/_changes?filter=sync_gateway/bychannel&channels=abc,def", "", true)
	require.NoError(t, err, "Error waiting for changes")
	base.RequireAllAssertions(t,
		assert.Equal(t, "doc1", changes.Results[0].ID),
		assert.Equal(t, "doc2", changes.Results[1].ID),
	)

	// Purge "doc1"
	resp := rt.SendAdminRequest("POST", "/db/_purge", `{"doc1":["*"]}`)
	rest.RequireStatus(t, resp, http.StatusOK)
	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &body))
	assert.Equal(t, map[string]interface{}{"purged": map[string]interface{}{"doc1": []interface{}{"*"}}}, body)

	changes, err = rt.WaitForChanges(1, "/db/_changes?filter=sync_gateway/bychannel&channels=abc,def", "", true)
	require.NoError(t, err, "Error waiting for changes")
	assert.Equal(t, "doc2", changes.Results[0].ID)

}

func TestPurgeWithSomeInvalidDocs(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/db/doc2", `{"moo":"car"}`), 201)

	response := rt.SendAdminRequest("POST", "/db/_purge", `{"doc1":["*"],"doc2":["1-123"]}`)
	rest.RequireStatus(t, response, 200)
	var body map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, map[string]interface{}{"purged": map[string]interface{}{"doc1": []interface{}{"*"}}}, body)

	// Create new versions of the doc1 without conflicts
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/db/doc1", `{"foo":"bar"}`), 201)

	// Create new versions of the doc2 fails because it already exists
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/db/doc2", `{"moo":"car"}`), 409)
}

func TestRawRedaction(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	res := rt.SendAdminRequest("PUT", "/db/testdoc", `{"foo":"bar", "channels": ["achannel"]}`)
	rest.RequireStatus(t, res, http.StatusCreated)

	// Test redact being disabled by default
	res = rt.SendAdminRequest("GET", "/db/_raw/testdoc", ``)
	var body map[string]interface{}
	err := base.JSONUnmarshal(res.Body.Bytes(), &body)
	assert.NoError(t, err)
	syncData := body[base.SyncPropertyName]
	assert.Equal(t, map[string]interface{}{"achannel": nil}, syncData.(map[string]interface{})["channels"])
	assert.Equal(t, []interface{}([]interface{}{[]interface{}{"achannel"}}), syncData.(map[string]interface{})["history"].(map[string]interface{})["channels"])

	// Test redacted
	body = map[string]interface{}{}
	res = rt.SendAdminRequest("GET", "/db/_raw/testdoc?redact=true&include_doc=false", ``)
	err = base.JSONUnmarshal(res.Body.Bytes(), &body)
	assert.NoError(t, err)
	syncData = body[base.SyncPropertyName]
	require.NotNil(t, syncData)
	assert.NotEqual(t, map[string]interface{}{"achannel": nil}, syncData.(map[string]interface{})["channels"])
	assert.NotEqual(t, []interface{}([]interface{}{[]interface{}{"achannel"}}), syncData.(map[string]interface{})["history"].(map[string]interface{})["channels"])

	// Test include doc false doesn't return doc
	body = map[string]interface{}{}
	res = rt.SendAdminRequest("GET", "/db/_raw/testdoc?include_doc=false", ``)
	assert.NotContains(t, res.Body.String(), "foo")

	// Test doc is returned by default
	body = map[string]interface{}{}
	res = rt.SendAdminRequest("GET", "/db/_raw/testdoc", ``)
	err = base.JSONUnmarshal(res.Body.Bytes(), &body)
	assert.NoError(t, err)
	assert.Equal(t, body["foo"], "bar")

	// Test that you can't use include_doc and redact at the same time
	res = rt.SendAdminRequest("GET", "/db/_raw/testdoc?include_doc=true&redact=true", ``)
	rest.RequireStatus(t, res, http.StatusBadRequest)
}

func TestRawTombstone(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	const docID = "testdoc"

	// Create a doc
	resp := rt.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"foo":"bar"}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	revID := rest.RespRevID(t, resp)

	resp = rt.SendAdminRequest(http.MethodGet, "/db/_raw/"+docID, ``)
	assert.Equal(t, "application/json", resp.Header().Get("Content-Type"))
	assert.NotContains(t, string(resp.BodyBytes()), `"_id":"`+docID+`"`)
	assert.NotContains(t, string(resp.BodyBytes()), `"_rev":"`+revID+`"`)
	assert.Contains(t, string(resp.BodyBytes()), `"foo":"bar"`)
	assert.NotContains(t, string(resp.BodyBytes()), `"_deleted":true`)

	// Delete the doc
	resp = rt.SendAdminRequest(http.MethodDelete, "/db/"+docID+"?rev="+revID, ``)
	rest.RequireStatus(t, resp, http.StatusOK)
	revID = rest.RespRevID(t, resp)

	resp = rt.SendAdminRequest(http.MethodGet, "/db/_raw/"+docID, ``)
	assert.Equal(t, "application/json", resp.Header().Get("Content-Type"))
	assert.NotContains(t, string(resp.BodyBytes()), `"_id":"`+docID+`"`)
	assert.NotContains(t, string(resp.BodyBytes()), `"_rev":"`+revID+`"`)
	assert.NotContains(t, string(resp.BodyBytes()), `"foo":"bar"`)
	assert.Contains(t, string(resp.BodyBytes()), `"_deleted":true`)
}

func TestHandleCreateDB(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	server := "walrus:"
	bucket := "albums"
	kvTLSPort := 11207
	resource := fmt.Sprintf("/%s/", bucket)

	bucketConfig := rest.BucketConfig{Server: &server, Bucket: &bucket, KvTLSPort: kvTLSPort}
	dbConfig := &rest.DbConfig{BucketConfig: bucketConfig, SGReplicateEnabled: base.BoolPtr(false)}
	var respBody db.Body

	reqBody, err := base.JSONMarshal(dbConfig)
	assert.NoError(t, err, "Error unmarshalling changes response")

	resp := rt.SendAdminRequest(http.MethodPut, resource, string(reqBody))
	rest.RequireStatus(t, resp, http.StatusCreated)
	assert.Empty(t, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, resource, string(reqBody))
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.NoError(t, respBody.Unmarshal([]byte(resp.Body.String())))
	assert.Equal(t, bucket, respBody["db_name"].(string))
	assert.Equal(t, "Online", respBody["state"].(string))

	// Try to create database with bad JSON request body and simulate JSON
	// parsing error from the handler; handleCreateDB.
	reqBodyJson := `"server":"walrus:","pool":"default","bucket":"albums","kv_tls_port":11207`
	resp = rt.SendAdminRequest(http.MethodPut, "/photos/", reqBodyJson)
	rest.RequireStatus(t, resp, http.StatusBadRequest)
}

func TestHandlePutDbConfigWithBackticks(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Get database info before putting config.
	resp := rt.SendAdminRequest(http.MethodGet, "/backticks/", "")
	rest.RequireStatus(t, resp, http.StatusNotFound)

	// Create database with valid JSON config that contains sync function enclosed in backticks.
	syncFunc := `function(doc, oldDoc) { console.log("foo");}`
	reqBodyWithBackticks := `{
        "server": "walrus:",
        "bucket": "backticks",
        "sync": ` + "`" + syncFunc + "`" + `
	}`
	resp = rt.SendAdminRequest(http.MethodPut, "/backticks/", reqBodyWithBackticks)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Get database config after putting config.
	resp = rt.SendAdminRequest(http.MethodGet, "/backticks/_config?include_runtime=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	var respBody db.Body
	require.NoError(t, respBody.Unmarshal([]byte(resp.Body.String())))
	assert.Equal(t, "walrus:", respBody["server"].(string))
	assert.Equal(t, syncFunc, respBody["sync"].(string))
}

func TestHandleDBConfig(t *testing.T) {
	tb := base.GetTestBucket(t)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{CustomTestBucket: tb})
	defer rt.Close()

	bucket := tb.GetName()
	dbname := "db"
	resource := fmt.Sprintf("/%s/", dbname)

	// Get database config before putting any config.
	resp := rt.SendAdminRequest(http.MethodGet, resource, "")
	rest.RequireStatus(t, resp, http.StatusOK)
	var respBody db.Body
	assert.NoError(t, respBody.Unmarshal(resp.Body.Bytes()))
	assert.Nil(t, respBody["bucket"])
	assert.Equal(t, dbname, respBody["db_name"].(string))
	assert.Equal(t, "Online", respBody["state"].(string))

	// Put database config
	resource = resource + "_config"

	// change cache size so we can see the update being reflected in the API response
	dbConfig := &rest.DbConfig{
		BucketConfig: rest.BucketConfig{Bucket: &bucket},
		CacheConfig: &rest.CacheConfig{
			RevCacheConfig: &rest.RevCacheConfig{
				Size: base.Uint32Ptr(1337), ShardCount: base.Uint16Ptr(7),
			},
		},
		NumIndexReplicas:   base.UintPtr(0),
		EnableXattrs:       base.BoolPtr(base.TestUseXattrs()),
		UseViews:           base.BoolPtr(base.TestsDisableGSI()),
		SGReplicateEnabled: base.BoolPtr(false),
	}
	reqBody, err := base.JSONMarshal(dbConfig)
	assert.NoError(t, err, "Error unmarshalling changes response")
	resp = rt.SendAdminRequest(http.MethodPut, resource, string(reqBody))
	rest.RequireStatus(t, resp, http.StatusCreated)
	assert.Empty(t, resp.Body.String())

	// Get database config after putting valid database config
	resp = rt.SendAdminRequest(http.MethodGet, resource, "")
	rest.RequireStatus(t, resp, http.StatusOK)
	respBody = nil
	assert.NoError(t, respBody.Unmarshal(resp.Body.Bytes()))

	gotcache, ok := respBody["cache"].(map[string]interface{})
	require.True(t, ok)
	assert.NotNil(t, gotcache)

	gotRevcache, ok := gotcache["rev_cache"].(map[string]interface{})
	require.True(t, ok)
	gotRevcacheSize, ok := gotRevcache["size"].(json.Number)
	require.True(t, ok)
	gotRevcacheSizeInt, err := gotRevcacheSize.Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(1337), gotRevcacheSizeInt)

	gotRevcacheNumShards, ok := gotRevcache["shard_count"].(json.Number)
	require.True(t, ok)
	gotRevcacheNumShardsInt, err := gotRevcacheNumShards.Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(7), gotRevcacheNumShardsInt)

	gotbucket, ok := respBody["bucket"].(string)
	require.True(t, ok)
	assert.Equal(t, bucket, gotbucket)

	gotName, ok := respBody["name"].(string)
	require.True(t, ok)
	assert.Equal(t, dbname, gotName)

	un, _, _ := tb.BucketSpec.Auth.GetCredentials()
	gotusername, ok := respBody["username"].(string)
	require.True(t, ok)
	assert.Equal(t, un, gotusername)
	gotpassword, ok := respBody["password"].(string)
	require.True(t, ok)
	assert.Equal(t, base.RedactedStr, gotpassword)

	_, ok = respBody["certpath"]
	require.False(t, ok)
	_, ok = respBody["keypath"]
	require.False(t, ok)
	_, ok = respBody["cacertpath"]
	require.False(t, ok)
}

func TestHandleDeleteDB(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Try to delete the database which doesn't exists
	resp := rt.SendAdminRequest(http.MethodDelete, "/albums/", "{}")
	rest.RequireStatus(t, resp, http.StatusNotFound)
	assert.Contains(t, string(resp.BodyBytes()), "no such database")
	var v map[string]interface{}
	assert.NoError(t, json.Unmarshal(resp.BodyBytes(), &v), "couldn't unmarshal %s", string(resp.BodyBytes()))

	// Create the database
	resp = rt.SendAdminRequest(http.MethodPut, "/albums/", `{"server":"walrus:"}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	assert.Empty(t, resp.Body.String())

	// Delete the database
	resp = rt.SendAdminRequest(http.MethodDelete, "/albums/", "{}")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), "{}")
}

func TestHandleGetConfig(t *testing.T) {
	syncFunc := `function(doc) {throw({forbidden: "read only!"})}`
	conf := rest.RestTesterConfig{SyncFn: syncFunc}
	rt := rest.NewRestTester(t, &conf)
	defer rt.Close()

	resp := rt.SendAdminRequest(http.MethodGet, "/_config", "{}")
	rest.RequireStatus(t, resp, http.StatusOK)

	var respBody rest.StartupConfig
	assert.NoError(t, base.JSONUnmarshal([]byte(resp.Body.String()), &respBody))

	assert.Equal(t, "127.0.0.1:4985", respBody.API.AdminInterface)
}

func TestHandleGetRevTree(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Create three revisions of the user foo with different status and updated_at values;
	reqBodyJson := `{"new_edits": false, "docs": [
    	{"_id": "foo", "type": "user", "updated_at": "2016-06-24T17:37:49.715Z", "status": "online", "_rev": "1-123"}, 
    	{"_id": "foo", "type": "user", "updated_at": "2016-06-26T17:37:49.715Z", "status": "offline", "_rev": "1-456"}, 
    	{"_id": "foo", "type": "user", "updated_at": "2016-06-25T17:37:49.715Z", "status": "offline", "_rev": "1-789"}]}`

	resp := rt.SendAdminRequest(http.MethodPost, "/db/_bulk_docs", reqBodyJson)
	rest.RequireStatus(t, resp, http.StatusCreated)
	respBodyExpected := `[{"id":"foo","rev":"1-123"},{"id":"foo","rev":"1-456"},{"id":"foo","rev":"1-789"}]`
	assert.Equal(t, respBodyExpected, resp.Body.String())

	// Get the revision tree  of the user foo
	resp = rt.SendAdminRequest(http.MethodGet, "/db/_revtree/foo", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Contains(t, resp.Body.String(), "1-123")
	assert.Contains(t, resp.Body.String(), "1-456")
	assert.Contains(t, resp.Body.String(), "1-789")
	assert.True(t, strings.HasPrefix(resp.Body.String(), "digraph"))
}

func TestHandleSGCollect(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()
	reqBodyJson := "invalidjson"
	resource := "/_sgcollect_info"

	// Check SGCollect status before triggering it; status should be stopped if no process is running.
	resp := rt.SendAdminRequest(http.MethodGet, resource, reqBodyJson)
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.Equal(t, resp.Body.String(), `{"status":"stopped"}`)

	// Try to cancel SGCollect before triggering it; Error stopping sgcollect_info: not running
	resp = rt.SendAdminRequest(http.MethodDelete, resource, reqBodyJson)
	rest.RequireStatus(t, resp, http.StatusBadRequest)
	assert.Contains(t, resp.Body.String(), "Error stopping sgcollect_info: not running")

	// Try to start SGCollect with invalid body; It should throw with unexpected end of JSON input error
	resp = rt.SendAdminRequest(http.MethodPost, resource, reqBodyJson)
	rest.RequireStatus(t, resp, http.StatusBadRequest)
}

func TestSessionExpirationDateTimeFormat(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	authenticator := auth.NewAuthenticator(rt.Bucket(), nil, auth.DefaultAuthenticatorOptions())
	user, err := authenticator.NewUser("alice", "letMe!n", channels.BaseSetOf(t, "*"))
	assert.NoError(t, err, "Couldn't create new user")
	assert.NoError(t, authenticator.Save(user), "Couldn't save new user")

	var body db.Body
	response := rt.SendAdminRequest(http.MethodPost, "/db/_session", `{"name":"alice"}`)
	rest.RequireStatus(t, response, http.StatusOK)

	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	expires, err := time.Parse(time.RFC3339, body["expires"].(string))
	assert.NoError(t, err, "Couldn't parse session expiration datetime")
	assert.True(t, expires.Sub(time.Now()).Hours() <= 24, "Couldn't validate session expiration")

	sessionId := body["session_id"].(string)
	require.NotEmpty(t, sessionId, "Couldn't parse sessionID from response body")
	response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/db/_session/%s", sessionId), "")
	rest.RequireStatus(t, response, http.StatusOK)

	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	expires, err = time.Parse(time.RFC3339, body["expires"].(string))
	assert.NoError(t, err, "Couldn't parse session expiration datetime")
	assert.True(t, expires.Sub(time.Now()).Hours() <= 24, "Couldn't validate session expiration")
}

func TestUserAndRoleResponseContentType(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Create a user 'christopher' through PUT request with empty request body.
	var responseBody db.Body
	body := `{"email":"christopher@couchbase.com","password":"cGFzc3dvcmQ=","admin_channels":["foo", "bar"]}`
	response := rt.SendAdminRequest(http.MethodPut, "/db/_user/christopher", "")
	assert.Equal(t, http.StatusBadRequest, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))

	// Create a user 'charles' through POST request with empty request body.
	body = `{"email":"charles@couchbase.com","password":"cGFzc3dvcmQ=","admin_channels":["foo", "bar"]}`
	response = rt.SendAdminRequest(http.MethodPost, "/db/_user/charles", "")
	assert.Equal(t, http.StatusMethodNotAllowed, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))

	// Create a user 'alice' through PUT request.
	body = `{"email":"alice@couchbase.com","password":"cGFzc3dvcmQ=","admin_channels":["foo", "bar"]}`
	response = rt.SendAdminRequest(http.MethodPut, "/db/_user/alice", body)
	assert.Equal(t, http.StatusCreated, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Create another user 'bob' through POST request.
	body = `{"name":"bob","email":"bob@couchbase.com","password":"cGFzc3dvcmQ=","admin_channels":["foo", "bar"]}`
	response = rt.SendAdminRequest(http.MethodPost, "/db/_user/", body)
	assert.Equal(t, http.StatusCreated, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Get the user details of user 'alice' through GET request.
	response = rt.SendAdminRequest(http.MethodGet, "/db/_user/alice", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))

	// Get the list of users through GET request.
	var users []string
	response = rt.SendAdminRequest(http.MethodGet, "/db/_user/", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &users))
	assert.Subset(t, []string{"alice", "bob"}, users)

	// Check whether the /db/_user/bob resource exist on the server.
	response = rt.SendAdminRequest(http.MethodHead, "/db/_user/bob", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Get the list of users through HEAD request.
	response = rt.SendAdminRequest(http.MethodHead, "/db/_user/", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Delete user 'alice'
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_user/alice", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Delete GUEST user instead of disabling.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_user/GUEST", "")
	assert.Equal(t, http.StatusMethodNotAllowed, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))

	// Delete user 'eve' who doesn't exists at this point of time.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_user/eve", "")
	assert.Equal(t, http.StatusNotFound, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))

	// Create a new user and save to database to create user session.
	authenticator := auth.NewAuthenticator(rt.Bucket(), nil, auth.DefaultAuthenticatorOptions())
	user, err := authenticator.NewUser("eve", "cGFzc3dvcmQ=", channels.BaseSetOf(t, "*"))
	assert.NoError(t, err, "Couldn't create new user")
	assert.NoError(t, authenticator.Save(user), "Couldn't save new user")

	// Create user session to check delete session request.
	response = rt.SendAdminRequest(http.MethodPost, "/db/_session", `{"name":"eve"}`)
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))
	sessionId, _ := responseBody["session_id"].(string)
	require.NotEmpty(t, sessionId, "Couldn't parse sessionID from response body")

	// Delete user session using /db/_user/eve/_session/{sessionId}.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_user/eve/_session/"+sessionId, "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Create user session to check delete session request.
	response = rt.SendAdminRequest(http.MethodPost, "/db/_session", `{"name":"eve"}`)
	assert.Equal(t, http.StatusOK, response.Code)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))

	// Delete user session using /db/_user/eve/_session request.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_user/eve/_session", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Create a role 'developer' through POST request
	body = `{"name":"developer","admin_channels":["channel1", "channel2"]}`
	response = rt.SendAdminRequest(http.MethodPost, "/db/_role/", body)
	assert.Equal(t, http.StatusCreated, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Create another role 'coder' through PUT request.
	body = `{"admin_channels":["channel3", "channel4"]}`
	response = rt.SendAdminRequest(http.MethodPut, "/db/_role/coder", body)
	assert.Equal(t, http.StatusCreated, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Check whether the /db/_role/ resource exist on the server.
	response = rt.SendAdminRequest(http.MethodHead, "/db/_role/", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Get the created roles through GET request.
	var roles []string
	response = rt.SendAdminRequest(http.MethodGet, "/db/_role/", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &roles))
	assert.Subset(t, []string{"coder", "developer"}, roles)

	// Delete role 'coder' from database.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_role/coder", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Delete role who doesn't exist.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_role/programmer", "")
	assert.Equal(t, http.StatusNotFound, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))
}

func TestConfigRedaction(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{Users: map[string]*auth.PrincipalConfig{"alice": {Password: base.StringPtr("password")}}}}})
	defer rt.Close()

	// Test default db config redaction
	var unmarshaledConfig rest.DbConfig
	response := rt.SendAdminRequest("GET", "/db/_config?include_runtime=true", "")
	err := json.Unmarshal(response.BodyBytes(), &unmarshaledConfig)
	require.NoError(t, err)

	assert.Equal(t, base.RedactedStr, unmarshaledConfig.Password)
	assert.Equal(t, base.RedactedStr, *unmarshaledConfig.Users["alice"].Password)

	// Test default server config redaction
	var unmarshaledServerConfig rest.StartupConfig
	response = rt.SendAdminRequest("GET", "/_config?include_runtime=true", "")
	err = json.Unmarshal(response.BodyBytes(), &unmarshaledServerConfig)
	require.NoError(t, err)

	assert.Equal(t, base.RedactedStr, unmarshaledServerConfig.Bootstrap.Password)
}

// Reproduces panic seen in CBG-1053
func TestAdhocReplicationStatus(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll, base.KeyReplicate)
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SgReplicateEnabled: true})
	defer rt.Close()

	srv := httptest.NewServer(rt.TestAdminHandler())
	defer srv.Close()

	replConf := `
	{
	  "replication_id": "pushandpull-with-target-oneshot-adhoc",
	  "remote": "` + srv.URL + `/db",
	  "direction": "pushAndPull",
	  "adhoc": true
	}`

	resp := rt.SendAdminRequest("PUT", "/db/_replication/pushandpull-with-target-oneshot-adhoc", replConf)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// With the error hitting the replicationStatus endpoint will either return running, if not completed, and once
	// completed panics. With the fix after running it'll return a 404 as replication no longer exists.
	stateError := rt.WaitForCondition(func() bool {
		resp = rt.SendAdminRequest("GET", "/db/_replicationStatus/pushandpull-with-target-oneshot-adhoc", "")
		return resp.Code == http.StatusNotFound
	})
	assert.NoError(t, stateError)
}

func TestUserXattrsRawGet(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("Test requires xattrs to be enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

	docKey := t.Name()
	xattrKey := "xattrKey"

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				AutoImport:   true,
				UserXattrKey: xattrKey,
			},
		},
	})
	defer rt.Close()

	userXattrStore, ok := base.AsUserXattrStore(rt.Bucket())
	if !ok {
		t.Skip("Test requires Couchbase Bucket")
	}

	resp := rt.SendAdminRequest("PUT", "/db/"+docKey, "{}")
	rest.RequireStatus(t, resp, http.StatusCreated)
	require.NoError(t, rt.WaitForPendingChanges())

	_, err := userXattrStore.WriteUserXattr(docKey, xattrKey, "val")
	assert.NoError(t, err)

	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value() == 1
	})

	resp = rt.SendAdminRequest("GET", "/db/_raw/"+docKey, "")
	rest.RequireStatus(t, resp, http.StatusOK)

	var RawReturn struct {
		Meta struct {
			Xattrs map[string]interface{} `json:"xattrs"`
		} `json:"_meta"`
	}

	err = json.Unmarshal(resp.BodyBytes(), &RawReturn)

	assert.Equal(t, "val", RawReturn.Meta.Xattrs[xattrKey])
}

func TestRolePurge(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Create role
	resp := rt.SendAdminRequest("PUT", "/db/_role/role", `{"admin_channels":["channel"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Delete role
	resp = rt.SendAdminRequest("DELETE", "/db/_role/role", ``)
	rest.RequireStatus(t, resp, http.StatusOK)

	// Ensure role is gone
	resp = rt.SendAdminRequest("GET", "/db/_role/role", ``)
	rest.RequireStatus(t, resp, http.StatusNotFound)

	// Ensure role is 'soft-deleted' and we can still get the doc
	role, err := rt.GetDatabase().Authenticator(base.TestCtx(t)).GetRoleIncDeleted("role")
	assert.NoError(t, err)
	assert.NotNil(t, role)

	// Re-create role
	resp = rt.SendAdminRequest("PUT", "/db/_role/role", `{"admin_channels":["channel"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Delete role again but with purge flag
	resp = rt.SendAdminRequest("DELETE", "/db/_role/role?purge=true", ``)
	rest.RequireStatus(t, resp, http.StatusOK)

	// Ensure role is purged, can't access at all
	role, err = rt.GetDatabase().Authenticator(base.TestCtx(t)).GetRoleIncDeleted("role")
	assert.Nil(t, err)
	assert.Nil(t, role)

	// Ensure role returns 404 via REST call
	resp = rt.SendAdminRequest("GET", "/db/_role/role", ``)
	rest.RequireStatus(t, resp, http.StatusNotFound)
}

func TestSoftDeleteCasMismatch(t *testing.T) {
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip LeakyBucket test when running in integration")
	}

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Create role
	resp := rt.SendAdminRequest("PUT", "/db/_role/role", `{"admin_channels":["channel"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	leakyBucket, ok := base.AsLeakyBucket(rt.TestBucket)
	require.True(t, ok)

	// Set callback to trigger a DELETE AFTER an update. This will trigger a CAS mismatch.
	// Update is done on a GetRole operation so this delete is done between a GET and save operation.
	triggerCallback := true
	leakyBucket.SetPostUpdateCallback(func(key string) {
		if triggerCallback {
			triggerCallback = false
			resp = rt.SendAdminRequest("DELETE", "/db/_role/role", ``)
			rest.RequireStatus(t, resp, http.StatusOK)
		}
	})

	resp = rt.SendAdminRequest("PUT", "/db/_role/role", `{"admin_channels":["chan"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
}

func TestObtainUserChannelsForDeletedRoleCasFail(t *testing.T) {
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip LeakyBucket test when running in integration")
	}

	testCases := []struct {
		Name      string
		RunBefore bool
	}{
		{
			"Delete On GetUser",
			true,
		},
		{
			"Delete On InheritedChannels",
			false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			rt := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: `
			function(doc, oldDoc){
				if (doc._id === 'roleChannels'){
					access('role:role', doc.channels)
				}
				if (doc._id === 'userRoles'){
					role('user', doc.roles)
				}
			}
		`,
			})
			defer rt.Close()

			// Create role
			resp := rt.SendAdminRequest("PUT", "/db/_role/role", `{"admin_channels":["channel"]}`)
			rest.RequireStatus(t, resp, http.StatusCreated)

			// Create user
			resp = rt.SendAdminRequest("PUT", "/db/_user/user", `{"password": "pass"}`)
			rest.RequireStatus(t, resp, http.StatusCreated)

			// Add channel to role
			resp = rt.SendAdminRequest("PUT", "/db/roleChannels", `{"channels": "inherit"}`)
			rest.RequireStatus(t, resp, http.StatusCreated)

			// Add role to user
			resp = rt.SendAdminRequest("PUT", "/db/userRoles", `{"roles": "role:role"}`)
			rest.RequireStatus(t, resp, http.StatusCreated)

			leakyBucket, ok := base.AsLeakyBucket(rt.TestBucket)
			require.True(t, ok)

			triggerCallback := false
			leakyBucket.SetUpdateCallback(func(key string) {
				if triggerCallback {
					triggerCallback = false
					resp = rt.SendAdminRequest("DELETE", "/db/_role/role", ``)
					rest.RequireStatus(t, resp, http.StatusOK)
				}
			})

			if testCase.RunBefore {
				triggerCallback = true
			}

			authenticator := rt.GetDatabase().Authenticator(base.TestCtx(t))
			user, err := authenticator.GetUser("user")
			assert.NoError(t, err)

			if !testCase.RunBefore {
				triggerCallback = true
			}

			assert.Equal(t, []string{"!"}, user.InheritedChannels().AllKeys())

			// Ensure callback ran
			assert.False(t, triggerCallback)
		})

	}
}

// Test warnings being issued when a new channel is created with over 250 characters - CBG-1475
func TestChannelNameSizeWarningBoundaries(t *testing.T) {
	syncFn := "function sync(doc, oldDoc) { channel(doc.chan); }"
	var rt *rest.RestTester

	testCases := []struct {
		name                string
		warnThresholdLength uint32
		channelLength       int
		expectWarn          bool
	}{
		{
			name:                "Over max default channel length",
			warnThresholdLength: base.DefaultWarnThresholdChannelNameSize,
			channelLength:       int(base.DefaultWarnThresholdChannelNameSize) + 1,
			expectWarn:          true,
		},
		{
			name:                "Equal to max default channel length",
			warnThresholdLength: base.DefaultWarnThresholdChannelNameSize,
			channelLength:       int(base.DefaultWarnThresholdChannelNameSize),
			expectWarn:          false,
		},
		{
			name:                "Under max default channel length",
			warnThresholdLength: base.DefaultWarnThresholdChannelNameSize,
			channelLength:       int(base.DefaultWarnThresholdChannelNameSize) - 1,
			expectWarn:          false,
		},
		{
			name:                "Over max configured channel length",
			warnThresholdLength: 500,
			channelLength:       501,
			expectWarn:          true,
		},
		{
			name:                "Equal to max configured channel length",
			warnThresholdLength: 500,
			channelLength:       500,
			expectWarn:          false,
		},
		{
			name:                "Under max configured channel length",
			warnThresholdLength: 500,
			channelLength:       499,
			expectWarn:          false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			var thresholdConfig *db.WarningThresholds
			// If threshold is not default then configure it
			if test.warnThresholdLength != base.DefaultWarnThresholdChannelNameSize {
				thresholdConfig = &db.WarningThresholds{ChannelNameSize: &test.warnThresholdLength}
			}
			rt = rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: syncFn,
				DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
					Unsupported: &db.UnsupportedOptions{
						WarningThresholds: thresholdConfig,
					}},
				},
			})
			defer rt.Close()

			ctx := rt.Context()
			chanNameWarnCountBefore := rt.ServerContext().Database(ctx, "db").DbStats.Database().WarnChannelNameSizeCount.Value()

			docId := fmt.Sprintf("doc%v", test.channelLength)
			chanName := strings.Repeat("A", test.channelLength)
			tr := rt.SendAdminRequest("PUT", "/db/"+docId, `{"chan":"`+chanName+`"}`)

			rest.RequireStatus(t, tr, http.StatusCreated)
			chanNameWarnCountAfter := rt.ServerContext().Database(ctx, "db").DbStats.Database().WarnChannelNameSizeCount.Value()
			if test.expectWarn {
				assert.Equal(t, chanNameWarnCountBefore+1, chanNameWarnCountAfter)
			} else {
				assert.Equal(t, chanNameWarnCountBefore, chanNameWarnCountAfter)
			}
		})
	}
}
func TestChannelNameSizeWarningUpdateExistingDoc(t *testing.T) {
	syncFn := "function sync(doc, oldDoc) { channel(doc.chan); }"
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: syncFn,
	})
	defer rt.Close()

	// Update doc - should warn
	chanName := strings.Repeat("B", int(base.DefaultWarnThresholdChannelNameSize)+5)
	t.Run("Update doc without changing channel", func(t *testing.T) {
		tr := rt.SendAdminRequest("PUT", "/db/replace", `{"chan":"`+chanName+`"}`) // init doc
		rest.RequireStatus(t, tr, http.StatusCreated)

		ctx := rt.Context()
		before := rt.ServerContext().Database(ctx, "db").DbStats.Database().WarnChannelNameSizeCount.Value()
		revId := rest.RespRevID(t, tr)
		tr = rt.SendAdminRequest("PUT", "/db/replace?rev="+revId, `{"chan":"`+chanName+`", "data":"test"}`)
		rest.RequireStatus(t, tr, http.StatusCreated)
		after := rt.ServerContext().Database(ctx, "db").DbStats.Database().WarnChannelNameSizeCount.Value()
		assert.Equal(t, before+1, after)
	})
}
func TestChannelNameSizeWarningDocChannelUpdate(t *testing.T) {
	syncFn := "function sync(doc, oldDoc) { channel(doc.chan); }"
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: syncFn,
	})
	defer rt.Close()

	channelLength := int(base.DefaultWarnThresholdChannelNameSize) + 5
	// Update doc channel with creation of a new channel
	t.Run("Update doc with new channel", func(t *testing.T) {

		chanName := strings.Repeat("C", channelLength)
		tr := rt.SendAdminRequest("PUT", "/db/replaceNewChannel", `{"chan":"`+chanName+`"}`) // init doc
		rest.RequireStatus(t, tr, http.StatusCreated)

		ctx := rt.Context()
		before := rt.ServerContext().Database(ctx, "db").DbStats.Database().WarnChannelNameSizeCount.Value()
		revId := rest.RespRevID(t, tr)
		chanName = strings.Repeat("D", channelLength+5)
		tr = rt.SendAdminRequest("PUT", "/db/replaceNewChannel?rev="+revId, fmt.Sprintf(`{"chan":"`+chanName+`", "data":"test"}`))
		rest.RequireStatus(t, tr, http.StatusCreated)
		after := rt.ServerContext().Database(ctx, "db").DbStats.Database().WarnChannelNameSizeCount.Value()
		assert.Equal(t, before+1, after)
	})
}
func TestChannelNameSizeWarningDeleteChannel(t *testing.T) {
	syncFn := "function sync(doc, oldDoc) { channel(doc.chan); }"
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: syncFn,
	})
	defer rt.Close()

	channelLength := int(base.DefaultWarnThresholdChannelNameSize) + 5
	// Delete channel over max len - no warning
	t.Run("Delete channel over max length", func(t *testing.T) {
		chanName := strings.Repeat("F", channelLength)
		tr := rt.SendAdminRequest("PUT", "/db/deleteme", `{"chan":"`+chanName+`"}`) // init channel
		rest.RequireStatus(t, tr, http.StatusCreated)

		ctx := rt.Context()
		before := rt.ServerContext().Database(ctx, "db").DbStats.Database().WarnChannelNameSizeCount.Value()
		revId := rest.RespRevID(t, tr)
		tr = rt.SendAdminRequest("DELETE", "/db/deleteme?rev="+revId, "")
		rest.RequireStatus(t, tr, http.StatusOK)
		after := rt.ServerContext().Database(ctx, "db").DbStats.Database().WarnChannelNameSizeCount.Value()
		assert.Equal(t, before, after)
	})
}

func TestConfigEndpoint(t *testing.T) {
	testCases := []struct {
		Name              string
		Config            string
		ConsoleLevel      base.LogLevel
		ConsoleLogKeys    []string
		ExpectError       bool
		FileLoggerCheckFn func() bool
	}{
		{
			Name: "Set LogLevel and LogKeys",
			Config: `
			{
				"logging": {
					"console": {
						"log_level": "trace",
						"log_keys": ["Config"]
					}
				}
			}`,
			ConsoleLevel:   base.LevelTrace,
			ConsoleLogKeys: []string{"Config"},
			ExpectError:    false,
		},
		{
			Name: "Set LogLevel and multiple LogKeys",
			Config: `
			{
				"logging": {
					"console": {
						"log_level": "info",
						"log_keys": ["Config", "HTTP+"]
					}
				}
			}`,
			ConsoleLevel:   base.LevelInfo,
			ConsoleLogKeys: []string{"Config", "HTTP"},
			ExpectError:    false,
		},
		{
			Name: "Set Invalid Fields",
			Config: `
			{
				"logging": {
					"console": {
						"log_level": "info",
						"log_keys": ["Config", "HTTP+"]
					},
					"fake": {}
				}
			}`,
			ConsoleLevel:   base.LevelTrace,
			ConsoleLogKeys: []string{"Config"},
			ExpectError:    true,
		},
		{
			Name: "Set non-runtime configurable Fields",
			Config: `
			{
				"logging": {
					"console": {
						"log_level": "info",
						"log_keys": ["Config", "HTTP+"]
					}
				},
				"bootstrap": {
					"server": "couchbase://0.0.0.0"
				}
			}`,
			ConsoleLevel:   base.LevelTrace,
			ConsoleLogKeys: []string{"Config"},
			ExpectError:    true,
		},
		{
			Name: "Enable Error Logger",
			Config: `
			{
				"logging": {
					"console": {
						"log_level": "info",
						"log_keys": ["Config", "HTTP+"]
					},
					"error": {
						"enabled": true
					}
				}
			}`,
			ConsoleLevel:   base.LevelInfo,
			ConsoleLogKeys: []string{"Config", "HTTP"},
			ExpectError:    false,
			FileLoggerCheckFn: func() bool {
				return base.ErrorLoggerIsEnabled()
			},
		},
		{
			Name: "Enable All File Loggers",
			Config: `
			{
				"logging": {
					"console": {
						"log_level": "info",
						"log_keys": ["*"]
					},
					"error": {
						"enabled": true
					},
					"warn": {
						"enabled": true
					},
					"info": {
						"enabled": true
					},
					"debug": {
						"enabled": true
					},
					"trace": {
						"enabled": true
					},
					"stats": {
						"enabled": true
					}
				}
			}`,
			ConsoleLevel:   base.LevelInfo,
			ConsoleLogKeys: []string{"*"},
			ExpectError:    false,
			FileLoggerCheckFn: func() bool {
				return base.ErrorLoggerIsEnabled() && base.WarnLoggerIsEnabled() && base.InfoLoggerIsEnabled() &&
					base.DebugLoggerIsEnabled() && base.TraceLoggerIsEnabled() && base.StatsLoggerIsEnabled()
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

			base.InitializeMemoryLoggers()
			tempDir := os.TempDir()
			test := rest.DefaultStartupConfig(tempDir)
			err := test.SetupAndValidateLogging()
			assert.NoError(t, err)

			rt := rest.NewRestTester(t, nil)
			defer rt.Close()

			// By default disable all loggers
			base.EnableErrorLogger(false)
			base.EnableWarnLogger(false)
			base.EnableInfoLogger(false)
			base.EnableDebugLogger(false)
			base.EnableTraceLogger(false)
			base.EnableStatsLogger(false)

			// Request to _config
			resp := rt.SendAdminRequest("PUT", "/_config", testCase.Config)
			if testCase.ExpectError {
				rest.RequireStatus(t, resp, http.StatusBadRequest)
				t.Logf("got response: %s", resp.BodyBytes())
				return
			}

			rest.RequireStatus(t, resp, http.StatusOK)

			assert.Equal(t, testCase.ConsoleLevel, *base.ConsoleLogLevel())
			assert.Equal(t, testCase.ConsoleLogKeys, base.ConsoleLogKey().EnabledLogKeys())

			if testCase.FileLoggerCheckFn != nil {
				assert.True(t, testCase.FileLoggerCheckFn())
			}
		})
	}
}

func TestLoggingDeprecationWarning(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Create doc just to startup server and force any initial warnings
	resp := rt.SendAdminRequest("PUT", "/db/doc", "{}")
	rest.RequireStatus(t, resp, http.StatusCreated)

	warnCountBefore := base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value()

	resp = rt.SendAdminRequest("GET", "/_logging", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	warnCountAfter := base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value()
	assert.Equal(t, int64(1), warnCountAfter-warnCountBefore)

	resp = rt.SendAdminRequest("PUT", "/_logging", "{}")
	rest.RequireStatus(t, resp, http.StatusOK)

	warnCountAfter2 := base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value()
	assert.Equal(t, int64(1), warnCountAfter2-warnCountAfter)

}

func TestInitialStartupConfig(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Get config
	resp := rt.SendAdminRequest("GET", "/_config", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	var initialStartupConfig rest.StartupConfig
	err := json.Unmarshal(resp.BodyBytes(), &initialStartupConfig)
	require.NoError(t, err)

	// Assert on a couple values to make sure they are set
	assert.Equal(t, base.TestClusterUsername(), initialStartupConfig.Bootstrap.Username)
	assert.Equal(t, base.RedactedStr, initialStartupConfig.Bootstrap.Password)

	// Assert error logging is nil
	assert.Nil(t, initialStartupConfig.Logging.Error)

	// Set logging running config
	rt.ServerContext().Config.Logging.Error = &base.FileLoggerConfig{}

	// Get config
	resp = rt.SendAdminRequest("GET", "/_config", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	initialStartupConfig = rest.StartupConfig{}
	err = json.Unmarshal(resp.BodyBytes(), &initialStartupConfig)
	require.NoError(t, err)

	// Assert that error logging is still nil, that the above running config didn't change anything
	assert.Nil(t, initialStartupConfig.Logging.Error)
}

func TestIncludeRuntimeStartupConfig(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	base.InitializeMemoryLoggers()
	tempDir := os.TempDir()
	test := rest.DefaultStartupConfig(tempDir)
	err := test.SetupAndValidateLogging()
	assert.NoError(t, err)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	base.EnableErrorLogger(false)
	base.EnableWarnLogger(false)
	base.EnableInfoLogger(false)
	base.EnableDebugLogger(false)
	base.EnableTraceLogger(false)
	base.EnableStatsLogger(false)

	// Get config
	resp := rt.SendAdminRequest("GET", "/_config?include_runtime=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	var runtimeServerConfigResponse rest.RunTimeServerConfigResponse
	err = json.Unmarshal(resp.BodyBytes(), &runtimeServerConfigResponse)
	require.NoError(t, err)

	assert.Contains(t, runtimeServerConfigResponse.Databases, "db")
	assert.Equal(t, base.UnitTestUrl(), runtimeServerConfigResponse.Bootstrap.Server)
	assert.Equal(t, base.TestClusterUsername(), runtimeServerConfigResponse.Bootstrap.Username)
	assert.Equal(t, base.RedactedStr, runtimeServerConfigResponse.Bootstrap.Password)

	// Make request to enable error logger
	resp = rt.SendAdminRequest("PUT", "/_config", `
	{
		"logging": {
			"console": {
				"log_level": "debug",
				"log_keys": ["*"]
			},
			"error": {
				"enabled": true
			}
		}
	}
	`)
	rest.RequireStatus(t, resp, http.StatusOK)

	// Update revs limit too so we can check db config
	dbConfig := rt.ServerContext().GetDatabaseConfig("db")
	dbConfig.RevsLimit = base.Uint32Ptr(100)

	resp = rt.SendAdminRequest("GET", "/_config?include_runtime=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	err = json.Unmarshal(resp.BodyBytes(), &runtimeServerConfigResponse)
	require.NoError(t, err)

	// Check that db revs limit is there now and error logging config
	assert.Contains(t, runtimeServerConfigResponse.Databases, "db")
	assert.Equal(t, base.Uint32Ptr(100), runtimeServerConfigResponse.Databases["db"].RevsLimit)

	assert.NotNil(t, runtimeServerConfigResponse.Logging.Error)
	assert.Equal(t, "debug", runtimeServerConfigResponse.Logging.Console.LogLevel.String())

	resp = rt.SendAdminRequest("GET", "/_config?include_runtime=true&redact=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	err = json.Unmarshal(resp.BodyBytes(), &runtimeServerConfigResponse)
	require.NoError(t, err)

	assert.Equal(t, base.RedactedStr, runtimeServerConfigResponse.Bootstrap.Password)

	// Setup replication to ensure it is visible in returned config
	replicationConfig := `{
		"replication_id": "repl",
		"remote": "http://remote:4985/db",
		"direction":"` + db.ActiveReplicatorTypePushAndPull + `",
		"conflict_resolution_type":"default",
		"max_backoff":100
	}`

	response := rt.SendAdminRequest("PUT", "/db/_replication/repl", string(replicationConfig))
	rest.RequireStatus(t, response, http.StatusCreated)

	resp = rt.SendAdminRequest("GET", "/_config?include_runtime=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	err = json.Unmarshal(resp.BodyBytes(), &runtimeServerConfigResponse)
	require.NoError(t, err)

	require.Contains(t, runtimeServerConfigResponse.Databases, "db")
	require.Contains(t, runtimeServerConfigResponse.Databases["db"].Replications, "repl")
	replCfg := runtimeServerConfigResponse.Databases["db"].Replications["repl"]
	assert.Equal(t, "repl", replCfg.ID)
	assert.Equal(t, "http://remote:4985/db", replCfg.Remote)
	assert.Equal(t, db.ActiveReplicatorTypePushAndPull, replCfg.Direction)

}

func TestPersistentConfigConcurrency(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	config := rest.BootstrapStartupConfigForTest(t)
	ctx := base.TestCtx(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	// Get config
	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config", "")
	resp.RequireStatus(http.StatusOK)
	eTag := resp.Header.Get("ETag")
	unquoteETag := strings.Trim(eTag, `"`)
	assert.NotEqual(t, "", unquoteETag)

	resp = rest.BootstrapAdminRequestWithHeaders(t, http.MethodPost, "/db/_config", "{}", map[string]string{"If-Match": eTag})
	resp.RequireStatus(http.StatusCreated)

	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_config", "{}")
	resp.RequireStatus(http.StatusCreated)
	putETag := resp.Header.Get("ETag")
	assert.NotEqual(t, "", putETag)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config", "")
	resp.RequireStatus(http.StatusOK)
	getETag := resp.Header.Get("ETag")
	assert.Equal(t, putETag, getETag)

	quotedStr := `"x"`
	resp = rest.BootstrapAdminRequestWithHeaders(t, http.MethodPost, "/db/_config", "{}", map[string]string{"If-Match": quotedStr})
	resp.RequireStatus(http.StatusPreconditionFailed)
}

func TestDbConfigCredentials(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	var dbConfig rest.DatabaseConfig

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config", "")
	resp.RequireStatus(http.StatusOK)
	err = base.JSONUnmarshal([]byte(resp.Body), &dbConfig)
	require.NoError(t, err)

	// non-runtime config, we don't expect to see any credentials present
	assert.Equal(t, "", dbConfig.Username)
	assert.Equal(t, "", dbConfig.Password)
	assert.Equal(t, "", dbConfig.CACertPath)
	assert.Equal(t, "", dbConfig.CertPath)
	assert.Equal(t, "", dbConfig.KeyPath)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config?include_runtime=true", "")
	resp.RequireStatus(http.StatusOK)
	err = base.JSONUnmarshal([]byte(resp.Body), &dbConfig)
	require.NoError(t, err)

	// runtime config, we expect to see the credentials used by the database (either bootstrap or per-db - but in this case, bootstrap)
	assert.Equal(t, base.TestClusterUsername(), dbConfig.Username)
	assert.Equal(t, base.RedactedStr, dbConfig.Password)
	assert.Equal(t, "", dbConfig.CACertPath)
	assert.Equal(t, "", dbConfig.CertPath)
	assert.Equal(t, "", dbConfig.KeyPath)
}

func TestInvalidDBConfig(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	// Put db config with invalid sync fn
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config", `{"sync": "function(){"}`)
	resp.RequireStatus(http.StatusBadRequest)
	assert.True(t, strings.Contains(resp.Body, "invalid javascript syntax"))

	// Put invalid sync fn via sync specific endpoint
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config/sync", `function(){`)
	resp.RequireStatus(http.StatusBadRequest)
	assert.True(t, strings.Contains(resp.Body, "invalid javascript syntax"))

	// Put invalid import fn via import specific endpoint
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config/import_filter", `function(){`)
	resp.RequireStatus(http.StatusBadRequest)
	assert.True(t, strings.Contains(resp.Body, "invalid javascript syntax"))
}

func TestCreateDbOnNonExistentBucket(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/", `{"bucket": "nonexistentbucket"}`)
	resp.RequireStatus(http.StatusForbidden)
	assert.Contains(t, resp.Body, "auth failure accessing provided bucket: nonexistentbucket")

	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/nonexistentbucket/", `{}`)
	resp.RequireStatus(http.StatusForbidden)
	assert.Contains(t, resp.Body, "auth failure accessing provided bucket: nonexistentbucket")
}

func TestPutDbConfigChangeName(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config", `{"name": "test"}`)
	resp.RequireStatus(http.StatusBadRequest)
}

func TestPutDBConfigOIDC(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	// Attempt to update the config with an invalid OIDC issuer - should fail
	invalidOIDCConfig := fmt.Sprintf(
		`{
			"bucket": "%s",
			"num_index_replicas": 0,
			"enable_shared_bucket_access": %t,
			"use_views": %t,
			"oidc": {
				"providers": {
					"test": {
						"issuer": "https://test.invalid",
						"client_id": "test"
					}
				}
			}
		}`,
		tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
	)

	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config", invalidOIDCConfig)
	resp.RequireStatus(http.StatusBadRequest)

	// Now pass the parameter to skip the validation
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config?disable_oidc_validation=true", invalidOIDCConfig)
	resp.RequireStatus(http.StatusCreated)

	// Now check with a valid OIDC issuer
	validOIDCConfig := fmt.Sprintf(
		`{
			"bucket": "%s",
			"num_index_replicas": 0,
			"enable_shared_bucket_access": %t,
			"use_views": %t,
			"unsupported": {
				"oidc_test_provider": {
					"enabled": true
				}
			},
			"oidc": {
				"providers": {
					"test": {
						"issuer": "http://localhost:%d/db/_oidc_testing",
						"client_id": "sync_gateway"
					}
				}
			}
		}`,
		tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(), 4984+rest.BootstrapTestPortOffset,
	)

	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config", validOIDCConfig)
	resp.RequireStatus(http.StatusCreated)
}

func TestNotExistentDBRequest(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{AdminInterfaceAuthentication: true})
	defer rt.Close()

	eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	rest.MakeUser(t, httpClient, eps[0], "random", "password", nil)
	defer rest.DeleteUser(t, httpClient, eps[0], "random")

	// Request to non-existent db with valid credentials
	resp := rt.SendAdminRequestWithAuth("PUT", "/dbx/_config", "", "random", "password")
	rest.RequireStatus(t, resp, http.StatusForbidden)

	// Request to non-existent db with invalid credentials
	resp = rt.SendAdminRequestWithAuth("PUT", "/dbx/_config", "", "random", "passwordx")
	rest.RequireStatus(t, resp, http.StatusUnauthorized)
}

func TestConfigsIncludeDefaults(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	var dbConfig rest.DatabaseConfig
	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config?include_runtime=true", "")
	resp.RequireStatus(http.StatusOK)
	err = base.JSONUnmarshal([]byte(resp.Body), &dbConfig)
	assert.NoError(t, err)

	// Validate a few default values to ensure they are set
	assert.Equal(t, channels.DefaultSyncFunction, *dbConfig.Sync)
	assert.Equal(t, db.DefaultChannelCacheMaxNumber, *dbConfig.CacheConfig.ChannelCacheConfig.MaxNumber)
	assert.Equal(t, base.DefaultOldRevExpirySeconds, *dbConfig.OldRevExpirySeconds)
	assert.Equal(t, false, *dbConfig.StartOffline)
	assert.Equal(t, db.DefaultCompactInterval, uint32(*dbConfig.CompactIntervalDays))

	var runtimeServerConfigResponse rest.RunTimeServerConfigResponse
	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/_config?include_runtime=true", "")
	resp.RequireStatus(http.StatusOK)
	err = base.JSONUnmarshal([]byte(resp.Body), &runtimeServerConfigResponse)
	assert.NoError(t, err)

	require.Contains(t, runtimeServerConfigResponse.Databases, "db")
	runtimeServerConfigDatabase := runtimeServerConfigResponse.Databases["db"]
	assert.Equal(t, channels.DefaultSyncFunction, *runtimeServerConfigDatabase.Sync)
	assert.Equal(t, db.DefaultChannelCacheMaxNumber, *runtimeServerConfigDatabase.CacheConfig.ChannelCacheConfig.MaxNumber)
	assert.Equal(t, base.DefaultOldRevExpirySeconds, *runtimeServerConfigDatabase.OldRevExpirySeconds)
	assert.Equal(t, false, *runtimeServerConfigDatabase.StartOffline)
	assert.Equal(t, db.DefaultCompactInterval, uint32(*runtimeServerConfigDatabase.CompactIntervalDays))

	// Test unsupported options
	tb2 := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket 2")
		tb2.Close()
	}()
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db2/",
		`{"bucket": "`+tb2.GetName()+`", "num_index_replicas": 0, "unsupported": {"disable_clean_skipped_query": true}}`,
	)
	resp.RequireStatus(http.StatusCreated)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/_config?include_runtime=true", "")
	resp.RequireStatus(http.StatusOK)
	err = base.JSONUnmarshal([]byte(resp.Body), &runtimeServerConfigResponse)
	assert.NoError(t, err)

	require.Contains(t, runtimeServerConfigResponse.Databases, "db2")
	runtimeServerConfigDatabase = runtimeServerConfigResponse.Databases["db2"]
	assert.True(t, runtimeServerConfigDatabase.Unsupported.DisableCleanSkippedQuery)
}

func TestLegacyCredentialInheritance(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, false)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() { tb.Close() }()

	// No credentials should fail
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db1/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusForbidden)

	// Wrong credentials should fail
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db2/",
		`{"bucket": "`+tb.GetName()+`", "username": "test", "password": "invalid_password"}`,
	)
	resp.RequireStatus(http.StatusForbidden)

	// Proper credentials should pass
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db3/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t, "username": "%s", "password": "%s"}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(), base.TestClusterUsername(), base.TestClusterPassword(),
		),
	)
	resp.RequireStatus(http.StatusCreated)
}

func TestDbOfflineConfigLegacy(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	bucket := rt.Bucket()
	defer rt.Close()

	dbConfig := `{
	"bucket": "` + bucket.GetName() + `",
	"name": "db",
	"sync": "function(doc){ channel(doc.channels); }",
	"import_filter": "function(doc) { return true }",
	"import_docs": false,
	"offline": false,
	"enable_shared_bucket_access": ` + strconv.FormatBool(base.TestUseXattrs()) + `,
	"use_views": ` + strconv.FormatBool(base.TestsDisableGSI()) + `,
	"num_index_replicas": 0 }`

	// Persist config
	resp := rt.SendAdminRequest("PUT", "/db/_config", dbConfig)
	require.Equal(t, http.StatusCreated, resp.Code)

	// Get config values before taking db offline
	resp = rt.SendAdminRequest("GET", "/db/_config", "")
	require.Equal(t, http.StatusOK, resp.Code)
	dbConfigBeforeOffline := string(resp.BodyBytes())

	// Take DB offline
	resp = rt.SendAdminRequest("POST", "/db/_offline", "")
	require.Equal(t, http.StatusOK, resp.Code)

	// Check offline config matches online config
	resp = rt.SendAdminRequest("GET", "/db/_config", "")
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, dbConfigBeforeOffline, string(resp.BodyBytes()))
}

func TestDbOfflineConfigPersistent(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() { tb.Close() }()

	importFilter := "function(doc) { return true }"
	syncFunc := "function(doc){ channel(doc.channels); }"

	dbConfig := `{
	"bucket": "%s",
	"name": "db",
	"sync": "%s",
	"import_filter": "%s",
	"import_docs": false,
	"offline": false,
	"enable_shared_bucket_access": %t,
	"use_views": %t,
	"num_index_replicas": 0 }`
	dbConfig = fmt.Sprintf(dbConfig, tb.GetName(), syncFunc, importFilter, base.TestUseXattrs(), base.TestsDisableGSI())

	// Persist config
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/", dbConfig)
	resp.RequireStatus(http.StatusCreated)

	// Get config values before taking db offline
	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config", "")
	resp.RequireStatus(http.StatusOK)
	dbConfigBeforeOffline := resp.Body

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config/import_filter", "")
	resp.RequireResponse(http.StatusOK, importFilter)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config/sync", "")
	resp.RequireResponse(http.StatusOK, syncFunc)

	// Take DB offline
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_offline", "")
	resp.RequireStatus(http.StatusOK)

	// Check offline config matches online config
	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config", "")
	resp.RequireResponse(http.StatusOK, dbConfigBeforeOffline)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config/import_filter", "")
	resp.RequireResponse(http.StatusOK, importFilter)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config/sync", "")
	resp.RequireResponse(http.StatusOK, syncFunc)
}

// TestDbConfigPersistentSGVersions ensures that cluster-wide config updates are not applied to older nodes to avoid pushing invalid configuration.
func TestDbConfigPersistentSGVersions(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyConfig)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	config := rest.BootstrapStartupConfigForTest(t)

	// enable the background update worker for this test only
	config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(time.Millisecond * 250)

	ctx := base.TestCtx(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	ctx = sc.SetContextLogID(ctx, "initial")

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() { tb.Close() }()

	dbConfig := rest.DatabaseConfig{
		SGVersion: "", // leave empty to emulate what 3.0.0 would've written to the bucket
		DbConfig: rest.DbConfig{
			BucketConfig: rest.BucketConfig{
				Bucket: base.StringPtr(tb.GetName()),
			},
			Name:             "db",
			EnableXattrs:     base.BoolPtr(base.TestUseXattrs()),
			UseViews:         base.BoolPtr(base.TestsDisableGSI()),
			NumIndexReplicas: base.UintPtr(0),
			RevsLimit:        base.Uint32Ptr(123), // use RevsLimit to detect config changes
		},
	}
	dbConfig.Version, err = rest.GenerateDatabaseConfigVersionID("", &dbConfig.DbConfig)
	require.NoError(t, err)

	// initialise with db config
	_, err = sc.BootstrapContext.Connection.InsertConfig(tb.GetName(), t.Name(), dbConfig)
	require.NoError(t, err)

	assertRevsLimit := func(sc *rest.ServerContext, revsLimit uint32) {
		rest.WaitAndAssertCondition(t, func() bool {
			dbc, err := sc.GetDatabase(ctx, "db")
			if err != nil {
				t.Logf("expected database with RevsLimit=%v but got err=%v", revsLimit, err)
				return false
			}
			if dbc.RevsLimit != revsLimit {
				t.Logf("expected database with RevsLimit=%v but got %v", revsLimit, dbc.RevsLimit)
				return false
			}
			return true
		}, "expected database with RevsLimit=%v", revsLimit)
	}

	assertRevsLimit(sc, 123)

	writeRevsLimitConfigWithVersion := func(sc *rest.ServerContext, version string, revsLimit uint32) error {
		_, err = sc.BootstrapContext.Connection.UpdateConfig(tb.GetName(), t.Name(), func(rawBucketConfig []byte) (updatedConfig []byte, err error) {
			var db rest.DatabaseConfig
			if err := base.JSONUnmarshal(rawBucketConfig, &db); err != nil {
				return nil, err
			}
			db.SGVersion = version
			db.DbConfig.RevsLimit = base.Uint32Ptr(revsLimit)
			db.Version, err = rest.GenerateDatabaseConfigVersionID(db.Version, &db.DbConfig)
			if err != nil {
				return nil, err
			}
			return base.JSONMarshal(db)
		})
		return err
	}

	require.NoError(t, writeRevsLimitConfigWithVersion(sc, "", 456))
	assertRevsLimit(sc, 456)

	// should be allowed (as of writing current version is 3.1.0)
	require.NoError(t, writeRevsLimitConfigWithVersion(sc, "3.0.1", 789))
	assertRevsLimit(sc, 789)

	// shouldn't be applied to the already started node (as "5.4.3" is newer)
	warnsStart := base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value()
	require.NoError(t, writeRevsLimitConfigWithVersion(sc, "5.4.3", 654))
	rest.WaitAndAssertConditionTimeout(t, time.Second*10, func() bool {
		warns := base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value()
		return warns-warnsStart > 3
	}, "expected some warnings from trying to apply newer config")
	assertRevsLimit(sc, 789)

	// Shut down the first SG node
	sc.Close(ctx)
	require.NoError(t, <-serverErr)

	// Start a new SG node and ensure we *can* load the "newer" config version on initial startup, to support downgrade
	sc, err = rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	ctx = sc.SetContextLogID(ctx, "newerconfig")
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	assertRevsLimit(sc, 654)

	// overwrite new config back to current version post-downgrade
	require.NoError(t, writeRevsLimitConfigWithVersion(sc, "3.1.0", 321))
	assertRevsLimit(sc, 321)
}

func TestDeleteFunctionsWhileDbOffline(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	// Start SG with bootstrap credentials filled
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	serverErr := make(chan error, 0)
	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	// Get a test bucket, and use it to create the database.
	// FIXME: CBG-2266 this test reads in persistent config
	tb := base.GetTestBucketDefaultCollection(t)
	defer func() { tb.Close() }()

	// Initial DB config
	dbConfig := `{
	"bucket": "` + tb.GetName() + `",
	"name": "db",
	"sync": "function(doc){ throw({forbidden : \"Rejected document\"}) }",
	"offline": false,
	"import_filter": "function(doc) { return false }",
	"import_docs": ` + strconv.FormatBool(base.TestUseXattrs()) + `,
	"enable_shared_bucket_access": ` + strconv.FormatBool(base.TestUseXattrs()) + `,
	"use_views": ` + strconv.FormatBool(base.TestsDisableGSI()) + `,
	"num_index_replicas": 0 }`

	// Create initial database
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/", dbConfig)
	resp.RequireStatus(http.StatusCreated)

	// Make sure import and sync fail
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/TestSyncDoc", "{}")
	resp.RequireStatus(http.StatusForbidden)

	// Take DB offline
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_offline", "")
	resp.RequireStatus(http.StatusOK)

	resp = rest.BootstrapAdminRequest(t, http.MethodDelete, "/db/_config/sync", "")
	resp.RequireStatus(http.StatusOK)

	// Take DB online
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_online", "")
	resp.RequireStatus(http.StatusOK)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config/sync", "")
	resp.RequireResponse(http.StatusOK, "")

	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/TestSyncDoc", "{}")
	resp.RequireStatus(http.StatusCreated)

	if base.TestUseXattrs() {
		add, err := tb.Add("TestImportDoc", 0, db.Document{ID: "TestImportDoc", RevID: "1-abc"})
		require.NoError(t, err)
		require.Equal(t, true, add)

		// On-demand import - rejected doc
		resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/TestImportDoc", "")
		resp.RequireStatus(http.StatusNotFound)

		// Persist configs
		resp = rest.BootstrapAdminRequest(t, http.MethodDelete, "/db/_config/import_filter", "")
		resp.RequireStatus(http.StatusOK)

		// Check configs match
		resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config/import_filter", "")
		resp.RequireResponse(http.StatusOK, "")

		// On-demand import - allowed doc after restored default import filter
		resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/TestImportDoc", "")
		resp.RequireStatus(http.StatusOK)
	}
}

func TestSetFunctionsWhileDbOffline(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	// Start SG with bootstrap credentials filled
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	serverErr := make(chan error, 0)
	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() { tb.Close() }()

	importFilter := "function(doc){ return true; }"
	syncFunc := "function(doc){ channel(doc.channels); }"

	// Initial DB config
	dbConfig := `{
	"bucket": "` + tb.GetName() + `",
	"name": "db",
	"offline": false,
	"enable_shared_bucket_access": ` + strconv.FormatBool(base.TestUseXattrs()) + `,
	"use_views": ` + strconv.FormatBool(base.TestsDisableGSI()) + `,
	"num_index_replicas": 0 }`

	// Create initial database
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/", dbConfig)
	resp.RequireStatus(http.StatusCreated)

	// Take DB offline
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_offline", "")
	resp.RequireStatus(http.StatusOK)

	// Persist configs
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config/import_filter", importFilter)
	resp.RequireStatus(http.StatusOK)

	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config/sync", syncFunc)
	resp.RequireStatus(http.StatusOK)

	// Take DB online
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_online", "")
	resp.RequireStatus(http.StatusOK)

	// Check configs match
	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config/import_filter", "")
	resp.RequireResponse(http.StatusOK, importFilter)

	resp = rest.BootstrapAdminRequest(t, http.MethodGet, "/db/_config/sync", "")
	resp.RequireResponse(http.StatusOK, syncFunc)
}

func TestEmptyStringJavascriptFunctions(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	serverErr := make(chan error, 0)

	// Start SG with no databases
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()

	// db put with empty sync func and import filter
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t, "sync": "", "import_filter": ""}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	// db config put with empty sync func and import filter
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/_config",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t, "sync": "", "import_filter": ""}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)

	// db config post, with empty sync func and import filter
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_config",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t, "sync": "", "import_filter": ""}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.RequireStatus(http.StatusCreated)
}

// Regression test for CBG-2119 - ensure that the disable_password_auth bool field is handled correctly both when set as true and as false
func TestDisablePasswordAuthThroughAdminAPI(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{})
	defer rt.Close()

	res := rt.SendAdminRequest(http.MethodPost, "/db/_config", `{"bucket":"`+rt.Bucket().GetName()+`","num_index_replicas":0,"disable_password_auth": true}`)
	rest.RequireStatus(t, res, http.StatusCreated)
	assert.True(t, rt.GetDatabase().Options.DisablePasswordAuthentication)

	res = rt.SendAdminRequest(http.MethodPost, "/db/_config", `{"bucket":"`+rt.Bucket().GetName()+`","num_index_replicas":0,"disable_password_auth": false}`)
	rest.RequireStatus(t, res, http.StatusCreated)
	assert.False(t, rt.GetDatabase().Options.DisablePasswordAuthentication)
}

// Tests replications to make sure they are namespaced by group ID
func TestGroupIDReplications(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || !base.TestUseXattrs() {
		t.Skip("This test only works against Couchbase Server with xattrs enabled")
	}
	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	// FIXME: CBG-2266 this test reads in persistent config

	// Create test buckets to replicate between
	passiveBucket := base.GetTestBucketDefaultCollection(t)
	defer passiveBucket.Close()

	activeBucket := base.GetTestBucketDefaultCollection(t)
	defer activeBucket.Close()

	// Set up passive bucket RT
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{CustomTestBucket: passiveBucket})
	defer rt.Close()

	// Make rt listen on an actual HTTP port, so it can receive replications
	srv := httptest.NewServer(rt.TestAdminHandler())
	defer srv.Close()
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Start SG nodes for default group, group A and group B
	groupIDs := []string{"", "GroupA", "GroupB"}
	var adminHosts []string
	var serverContexts []*rest.ServerContext
	for i, group := range groupIDs {
		serverErr := make(chan error, 0)

		config := rest.BootstrapStartupConfigForTest(t)
		portOffset := i * 10
		adminInterface := fmt.Sprintf("127.0.0.1:%d", 4985+rest.BootstrapTestPortOffset+portOffset)
		adminHosts = append(adminHosts, "http://"+adminInterface)
		config.API.PublicInterface = fmt.Sprintf("127.0.0.1:%d", 4984+rest.BootstrapTestPortOffset+portOffset)
		config.API.AdminInterface = adminInterface
		config.API.MetricsInterface = fmt.Sprintf("127.0.0.1:%d", 4986+rest.BootstrapTestPortOffset+portOffset)
		config.Bootstrap.ConfigGroupID = group
		if group == "" {
			config.Bootstrap.ConfigGroupID = rest.PersistentConfigDefaultGroupID
		}

		ctx := base.TestCtx(t)
		sc, err := rest.SetupServerContext(ctx, &config, true)
		require.NoError(t, err)
		serverContexts = append(serverContexts, sc)
		ctx = sc.SetContextLogID(ctx, config.Bootstrap.ConfigGroupID)
		defer func() {
			sc.Close(ctx)
			require.NoError(t, <-serverErr)
		}()
		go func() {
			serverErr <- rest.StartServer(ctx, &config, sc)
		}()
		require.NoError(t, sc.WaitForRESTAPIs())

		// Set up db config
		resp := rest.BootstrapAdminRequestCustomHost(t, http.MethodPut, adminHosts[i], "/db/",
			fmt.Sprintf(
				`{"bucket": "%s", "num_index_replicas": 0, "use_views": %t, "import_docs": true, "sync":"%s"}`,
				activeBucket.GetName(), base.TestsDisableGSI(), channels.DefaultSyncFunction,
			),
		)
		resp.RequireStatus(http.StatusCreated)
	}

	// Start replicators
	for i, group := range groupIDs {
		channelFilter := []string{"chan" + group}
		replicationConfig := db.ReplicationConfig{
			ID:                     "repl",
			Remote:                 passiveDBURL.String(),
			Direction:              db.ActiveReplicatorTypePush,
			Filter:                 base.ByChannelFilter,
			QueryParams:            map[string]interface{}{"channels": channelFilter},
			Continuous:             true,
			InitialState:           db.ReplicationStateRunning,
			ConflictResolutionType: db.ConflictResolverDefault,
		}
		resp := rest.BootstrapAdminRequestCustomHost(t, http.MethodPost, adminHosts[i], "/db/_replication/", rest.MarshalConfig(t, replicationConfig))
		resp.RequireStatus(http.StatusCreated)
	}

	for groupNum, group := range groupIDs {
		channel := "chan" + group
		key := "doc" + group
		body := fmt.Sprintf(`{"channels":["%s"]}`, channel)
		added, err := activeBucket.Add(key, 0, []byte(body))
		require.NoError(t, err)
		require.True(t, added)

		// Force on-demand import and cache
		for _, host := range adminHosts {
			resp := rest.BootstrapAdminRequestCustomHost(t, http.MethodGet, host, "/db/"+key, "")
			resp.RequireStatus(http.StatusOK)
		}

		for scNum, sc := range serverContexts {
			var expectedPushed int64 = 0
			// If replicated doc to db already (including this loop iteration) then expect 1
			if scNum <= groupNum {
				expectedPushed = 1
			}

			ctx := sc.SetContextLogID(base.TestCtx(t), sc.Config.Bootstrap.ConfigGroupID)
			dbContext, err := sc.GetDatabase(ctx, "db")
			require.NoError(t, err)
			dbstats, err := dbContext.DbStats.DBReplicatorStats("repl")
			require.NoError(t, err)
			actualPushed, _ := base.WaitForStat(dbstats.NumDocPushed.Value, expectedPushed)
			assert.Equal(t, expectedPushed, actualPushed)
		}
	}
}

// CBG-1790: Deleting a database that targets the same bucket as another causes a panic in legacy
func TestDeleteDatabasePointingAtSameBucket(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || !base.TestUseXattrs() {
		t.Skip("This test only works against Couchbase Server with xattrs")
	}
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)
	tb := base.GetTestBucket(t)
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{CustomTestBucket: tb})
	defer rt.Close()
	resp := rt.SendAdminRequest(http.MethodDelete, "/db/", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	// Make another database that uses import in-order to trigger the panic instantly instead of having to time.Sleep
	resp = rt.SendAdminRequest(http.MethodPut, "/db1/", fmt.Sprintf(`{
		"bucket": "%s",
		"username": "%s",
		"password": "%s",
		"use_views": %t,
		"num_index_replicas": 0
	}`, tb.GetName(), base.TestClusterUsername(), base.TestClusterPassword(), base.TestsDisableGSI()))
}

func TestDeleteDatabasePointingAtSameBucketPersistent(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || !base.TestUseXattrs() {
		t.Skip("This test only works against Couchbase Server with xattrs")
	}
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)
	// Start SG with no databases in bucket(s)
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	serverErr := make(chan error, 0)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()
	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())
	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()

	dbConfig := `{
   "bucket": "` + tb.GetName() + `",
   "name": "%s",
   "import_docs": true,
   "enable_shared_bucket_access": ` + strconv.FormatBool(base.TestUseXattrs()) + `,
   "use_views": ` + strconv.FormatBool(base.TestsDisableGSI()) + `,
   "num_index_replicas": 0 }`

	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db1/", fmt.Sprintf(dbConfig, "db1"))
	resp.RequireStatus(http.StatusCreated)

	resp = rest.BootstrapAdminRequest(t, http.MethodDelete, "/db1/", "")
	resp.RequireStatus(http.StatusOK)

	// Make another database that uses import in-order to trigger the panic instantly instead of having to time.Sleep
	resp = rest.BootstrapAdminRequest(t, http.MethodPut, "/db2/", fmt.Sprintf(dbConfig, "db2"))
	resp.RequireStatus(http.StatusCreated)

	// Validate that deleted database is no longer in dest factory set
	_, fetchDb1DestErr := base.FetchDestFactory(base.ImportDestKey("db1"))
	assert.Equal(t, base.ErrNotFound, fetchDb1DestErr)
	_, fetchDb2DestErr := base.FetchDestFactory(base.ImportDestKey("db2"))
	assert.NoError(t, fetchDb2DestErr)
}

// CBG-1046: Add ability to specify user for active peer in sg-replicate2
func TestSpecifyUserDocsToReplicate(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	testCases := []struct {
		direction string
	}{
		{
			direction: "push",
		},
		{
			direction: "pull",
		},
	}
	for _, test := range testCases {
		t.Run(test.direction, func(t *testing.T) {
			replName := test.direction
			syncFunc := `
function (doc) {
	if (doc.owner) {
		requireUser(doc.owner);
	}
	channel(doc.channels);
	requireAccess(doc.channels);
}`
			rtConfig := &rest.RestTesterConfig{
				SyncFn: syncFunc,
				DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
					Users: map[string]*auth.PrincipalConfig{
						"alice": {
							Password:         base.StringPtr("pass"),
							ExplicitChannels: base.SetOf("chanAlpha", "chanBeta", "chanCharlie", "chanHotel", "chanIndia"),
						},
						"bob": {
							Password:         base.StringPtr("pass"),
							ExplicitChannels: base.SetOf("chanDelta", "chanEcho"),
						},
					},
				}},
			}
			// Set up buckets, rest testers, and set up servers
			passiveRT := rest.NewRestTester(t, rtConfig)
			defer passiveRT.Close()

			publicSrv := httptest.NewServer(passiveRT.TestPublicHandler())
			defer publicSrv.Close()

			adminSrv := httptest.NewServer(passiveRT.TestAdminHandler())
			defer adminSrv.Close()

			activeRT := rest.NewRestTester(t, rtConfig)
			defer activeRT.Close()

			// Change RT depending on direction
			var senderRT *rest.RestTester   // RT that has the initial docs that get replicated to the other bucket
			var receiverRT *rest.RestTester // RT that gets the docs replicated to it
			if test.direction == "push" {
				senderRT = activeRT
				receiverRT = passiveRT
			} else if test.direction == "pull" {
				senderRT = passiveRT
				receiverRT = activeRT
			}

			// Create docs to replicate
			bulkDocsBody := `
{
  "docs": [
  	{"channels":["chanAlpha"], "access":"alice"},
  	{"channels":["chanBeta","chanFoxtrot"], "access":"alice"},
  	{"channels":["chanCharlie","chanEcho"], "access":"alice,bob"},
  	{"channels":["chanDelta"], "access":"bob"},
  	{"channels":["chanGolf"], "access":""},
  	{"channels":["!"], "access":"alice,bob"},
  	{"channels":["!"], "access":"bob", "owner":"bob"},
  	{"channels":["!"], "access":"alice", "owner":"alice"},
	{"channels":["chanHotel"], "access":"", "owner":"mike"},
	{"channels":["chanIndia"], "access":"alice", "owner":"alice"}
  ]
}
`
			resp := senderRT.SendAdminRequest("POST", "/db/_bulk_docs", bulkDocsBody)
			rest.RequireStatus(t, resp, http.StatusCreated)

			err := senderRT.WaitForPendingChanges()
			require.NoError(t, err)

			// Replicate just alices docs
			replConf := `
				{
					"replication_id": "` + replName + `",
					"remote": "` + publicSrv.URL + `/db",
					"direction": "` + test.direction + `",
					"continuous": true,
					"batch": 200,
					"run_as": "alice",
					"remote_username": "alice",
					"remote_password": "pass"
				}`

			resp = activeRT.SendAdminRequest("PUT", "/db/_replication/"+replName, replConf)
			rest.RequireStatus(t, resp, http.StatusCreated)

			activeCtx := activeRT.Context()
			err = activeRT.GetDatabase().SGReplicateMgr.StartReplications(activeCtx)
			require.NoError(t, err)
			activeRT.WaitForReplicationStatus(replName, db.ReplicationStateRunning)

			value, _ := base.WaitForStat(receiverRT.GetDatabase().DbStats.Database().NumDocWrites.Value, 6)
			assert.EqualValues(t, 6, value)

			changesResults, err := receiverRT.WaitForChanges(6, "/db/_changes?since=0&include_docs=true", "", true)
			assert.NoError(t, err)
			assert.Len(t, changesResults.Results, 6)
			// Check the docs are alices docs
			for _, result := range changesResults.Results {
				body, err := result.Doc.MarshalJSON()
				require.NoError(t, err)
				assert.Contains(t, string(body), "alice")
			}

			// Stop and remove replicator (to stop checkpointing after teardown causing panic)
			_, err = activeRT.GetDatabase().SGReplicateMgr.PutReplicationStatus(replName, "stop")
			require.NoError(t, err)
			activeRT.WaitForReplicationStatus(replName, db.ReplicationStateStopped)
			err = activeRT.GetDatabase().SGReplicateMgr.DeleteReplication(replName)
			require.NoError(t, err)

			// Replicate all docs
			// Run as admin should default to true
			replConf = `
					{
						"replication_id": "` + replName + `",
						"remote": "` + adminSrv.URL + `/db",
						"direction": "` + test.direction + `",
						"continuous": true,
						"batch": 200
					}`

			resp = activeRT.SendAdminRequest("PUT", "/db/_replication/"+replName, replConf)
			rest.RequireStatus(t, resp, http.StatusCreated)
			activeRT.WaitForReplicationStatus(replName, db.ReplicationStateRunning)

			value, _ = base.WaitForStat(receiverRT.GetDatabase().DbStats.Database().NumDocWrites.Value, 10)
			assert.EqualValues(t, 10, value)

			// Stop and remove replicator
			_, err = activeRT.GetDatabase().SGReplicateMgr.PutReplicationStatus(replName, "stop")
			require.NoError(t, err)
			activeRT.WaitForReplicationStatus(replName, db.ReplicationStateStopped)
			err = activeRT.GetDatabase().SGReplicateMgr.DeleteReplication(replName)
			require.NoError(t, err)
		})
	}
}

// Test that the username and password fields in the replicator still work and get redacted appropriately.
// This should log a deprecation notice.
func TestReplicatorDeprecatedCredentials(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	passiveRT := rest.NewRestTester(t, &rest.RestTesterConfig{DatabaseConfig: &rest.DatabaseConfig{
		DbConfig: rest.DbConfig{
			Users: map[string]*auth.PrincipalConfig{
				"alice": {
					Password: base.StringPtr("pass"),
				},
			},
		},
	},
	})
	defer passiveRT.Close()

	adminSrv := httptest.NewServer(passiveRT.TestPublicHandler())
	defer adminSrv.Close()

	activeRT := rest.NewRestTester(t, nil)
	defer activeRT.Close()
	activeCtx := activeRT.Context()

	err := activeRT.GetDatabase().SGReplicateMgr.StartReplications(activeCtx)
	require.NoError(t, err)

	rev := activeRT.CreateDoc(t, "test")

	replConfig := `
{
	"replication_id": "` + t.Name() + `",
	"remote": "` + adminSrv.URL + `/db",
	"direction": "push",
	"continuous": true,
	"username": "alice",
	"password": "pass"
}
`
	resp := activeRT.SendAdminRequest("POST", "/db/_replication/", replConfig)
	rest.RequireStatus(t, resp, 201)

	activeRT.WaitForReplicationStatus(t.Name(), db.ReplicationStateRunning)

	err = passiveRT.WaitForRev("test", rev)
	require.NoError(t, err)

	resp = activeRT.SendAdminRequest("GET", "/db/_replication/"+t.Name(), "")
	rest.RequireStatus(t, resp, 200)

	var config db.ReplicationConfig
	err = json.Unmarshal(resp.BodyBytes(), &config)
	require.NoError(t, err)
	assert.Equal(t, "alice", config.Username)
	assert.Equal(t, base.RedactedStr, config.Password)
	assert.Equal(t, "", config.RemoteUsername)
	assert.Equal(t, "", config.RemotePassword)

	_, err = activeRT.GetDatabase().SGReplicateMgr.PutReplicationStatus(t.Name(), "stop")
	require.NoError(t, err)
	activeRT.WaitForReplicationStatus(t.Name(), db.ReplicationStateStopped)
	err = activeRT.GetDatabase().SGReplicateMgr.DeleteReplication(t.Name())
	require.NoError(t, err)
}

// CBG-1581: Ensure activeReplicatorCommon does final checkpoint on stop/disconnect
func TestReplicatorCheckpointOnStop(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	passiveRT := rest.NewRestTester(t, nil)
	defer passiveRT.Close()

	adminSrv := httptest.NewServer(passiveRT.TestAdminHandler())
	defer adminSrv.Close()

	activeRT := rest.NewRestTester(t, nil)
	defer activeRT.Close()
	activeCtx := activeRT.Context()

	// Disable checkpointing at an interval
	activeRT.GetDatabase().SGReplicateMgr.CheckpointInterval = 0
	err := activeRT.GetDatabase().SGReplicateMgr.StartReplications(activeCtx)
	require.NoError(t, err)

	database, err := db.CreateDatabase(activeRT.GetDatabase())
	require.NoError(t, err)
	rev, doc, err := database.Put(activeCtx, "test", db.Body{})
	require.NoError(t, err)
	seq := strconv.FormatUint(doc.Sequence, 10)

	replConfig := `
{
	"replication_id": "` + t.Name() + `",
	"remote": "` + adminSrv.URL + `/db",
	"direction": "push",
	"continuous": true
}
`
	resp := activeRT.SendAdminRequest("POST", "/db/_replication/", replConfig)
	rest.RequireStatus(t, resp, 201)

	activeRT.WaitForReplicationStatus(t.Name(), db.ReplicationStateRunning)

	err = passiveRT.WaitForRev("test", rev)
	require.NoError(t, err)

	_, err = activeRT.GetDatabase().SGReplicateMgr.PutReplicationStatus(t.Name(), "stop")
	require.NoError(t, err)
	activeRT.WaitForReplicationStatus(t.Name(), db.ReplicationStateStopped)

	// Check checkpoint document was wrote to bucket with correct status
	// _sync:local:checkpoint/sgr2cp:push:TestReplicatorCheckpointOnStop
	expectedCheckpointName := base.SyncDocPrefix + "local:checkpoint/" + db.PushCheckpointID(t.Name())
	val, _, err := activeRT.Bucket().GetRaw(expectedCheckpointName)
	require.NoError(t, err)
	var config struct { // db.replicationCheckpoint
		LastSeq string `json:"last_sequence"`
	}
	err = json.Unmarshal(val, &config)
	require.NoError(t, err)
	assert.Equal(t, seq, config.LastSeq)

	err = activeRT.GetDatabase().SGReplicateMgr.DeleteReplication(t.Name())
	require.NoError(t, err)
}

func TestApiInternalPropertiesHandling(t *testing.T) {
	testCases := []struct {
		name                        string
		inputBody                   map[string]interface{}
		expectedErrorStatus         *int // If nil, will check for 201 Status Created
		skipDocContentsVerification *bool
	}{
		{
			name:      "Valid document with special prop",
			inputBody: map[string]interface{}{"_cookie": "is valid"},
		},
		{
			name:                "Invalid _sync",
			inputBody:           map[string]interface{}{"_sync": true},
			expectedErrorStatus: base.IntPtr(http.StatusBadRequest),
		},
		{
			name:                "Valid _sync",
			inputBody:           map[string]interface{}{"_sync": db.SyncData{}},
			expectedErrorStatus: base.IntPtr(http.StatusBadRequest),
		},
		{
			name:                        "Valid _deleted",
			inputBody:                   map[string]interface{}{"_deleted": false},
			skipDocContentsVerification: base.BoolPtr(true),
		},
		{
			name:                        "Valid _revisions",
			inputBody:                   map[string]interface{}{"_revisions": map[string]interface{}{"ids": "1-abc"}},
			skipDocContentsVerification: base.BoolPtr(true),
		},
		{
			name:                        "Valid _exp",
			inputBody:                   map[string]interface{}{"_exp": "123"},
			skipDocContentsVerification: base.BoolPtr(true),
		},
		{
			name:                "Invalid _exp",
			inputBody:           map[string]interface{}{"_exp": "abc"},
			expectedErrorStatus: base.IntPtr(http.StatusBadRequest),
		},
		{
			name:                "_purged",
			inputBody:           map[string]interface{}{"_purged": false},
			expectedErrorStatus: base.IntPtr(http.StatusBadRequest),
		},
		{
			name:                "_removed",
			inputBody:           map[string]interface{}{"_removed": false},
			expectedErrorStatus: base.IntPtr(http.StatusNotFound),
		},
		{
			name:                "_sync_cookies",
			inputBody:           map[string]interface{}{"_sync_cookies": true},
			expectedErrorStatus: base.IntPtr(http.StatusBadRequest),
		},
		{
			name: "Valid user defined uppercase properties", // Uses internal properties names but in upper case
			// Known issue: _SYNC causes unmarshal error when not using xattrs
			inputBody: map[string]interface{}{
				"_ID": true, "_REV": true, "_DELETED": true, "_ATTACHMENTS": true, "_REVISIONS": true,
				"_EXP": true, "_PURGED": true, "_REMOVED": true, "_SYNC_COOKIES": true,
			},
		},
	}

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	for i, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			docID := fmt.Sprintf("test%d", i)
			rawBody, err := json.Marshal(test.inputBody)
			require.NoError(t, err)

			resp := rt.SendAdminRequest("PUT", "/db/"+docID, string(rawBody))
			if test.expectedErrorStatus != nil {
				rest.RequireStatus(t, resp, *test.expectedErrorStatus)
				return
			}
			rest.RequireStatus(t, resp, http.StatusCreated)

			var bucketDoc map[string]interface{}
			_, err = rt.Bucket().Get(docID, &bucketDoc)
			assert.NoError(t, err)
			body := rt.GetDoc(docID)
			// Confirm input body is in the bucket doc
			if test.skipDocContentsVerification == nil || !*test.skipDocContentsVerification {
				for k, v := range test.inputBody {
					assert.Equal(t, v, bucketDoc[k])
					assert.Equal(t, v, body[k])
				}
			}
		})
	}
}

func TestPutIDRevMatchBody(t *testing.T) {
	// [REV] is replaced with the most recent revision of document "doc"
	testCases := []struct {
		name        string
		docBody     string
		docID       string
		rev         string
		expectError bool
	}{
		{
			name:        "ID match",
			docBody:     `{"_id": "id_match"}`,
			docID:       "id_match",
			expectError: false,
		},
		{
			name:        "ID mismatch",
			docBody:     `{"_id": "id_mismatch"}`,
			docID:       "completely_different_id",
			expectError: true,
		},
		{
			name:        "ID in URL only",
			docBody:     `{}`,
			docID:       "id_in_url",
			expectError: false,
		},
		{
			name:        "Rev match",
			docBody:     `{"_rev": "[REV]", "nonce": "1"}`,
			rev:         "[REV]",
			expectError: false,
		},
		{
			name:        "Rev mismatch",
			docBody:     `{"_rev": "[REV]", "nonce": "2"}`,
			rev:         "1-abc",
			expectError: true,
		},
		{
			name:        "Rev in body only",
			docBody:     `{"_rev": "[REV]", "nonce": "3"}`,
			expectError: false,
		},
		{
			name:        "Rev in URL only",
			docBody:     `{"nonce": "4"}`,
			rev:         "[REV]",
			expectError: false,
		},
	}

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()
	// Create document to create rev from
	resp := rt.SendAdminRequest("PUT", "/db/doc", "{}")
	rest.RequireStatus(t, resp, 201)
	rev := rest.RespRevID(t, resp)

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			docID := test.docID
			docRev := test.rev
			docBody := test.docBody
			if test.docID == "" {
				docID = "doc" // Used for the rev tests to branch off of
				docBody = strings.ReplaceAll(docBody, "[REV]", rev)
				docRev = strings.ReplaceAll(docRev, "[REV]", rev)
			}

			resp = rt.SendAdminRequest("PUT", "/db/"+docID+"?rev="+docRev, docBody)
			if test.expectError {
				rest.RequireStatus(t, resp, 400)
				return
			}
			rest.RequireStatus(t, resp, 201)
			if test.docID == "" {
				// Update rev to branch off for next test
				rev = rest.RespRevID(t, resp)
			}
		})
	}
}

func TestPublicChanGuestAccess(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			Guest: &auth.PrincipalConfig{
				Disabled: base.BoolPtr(false),
			},
		}},
	})
	defer rt.Close()

	// Create a document on the public channel
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc", `{"channels": ["!"], "foo": "bar"}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Check guest user has access to public channel
	resp = rt.SendRequest(http.MethodGet, "/db/doc", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	assert.EqualValues(t, "bar", resp.GetRestDocument()["foo"])

	resp = rt.SendAdminRequest(http.MethodGet, "/db/_user/GUEST", ``)
	rest.RequireStatus(t, resp, http.StatusOK)
	fmt.Println("GUEST user:", resp.Body.String())
	assert.EqualValues(t, []interface{}{"!"}, resp.GetRestDocument()["all_channels"])

	// Confirm guest user cannot access other channels it has no access too
	resp = rt.SendAdminRequest(http.MethodPut, "/db/docNoAccess", `{"channels": ["cookie"], "foo": "bar"}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendRequest(http.MethodGet, "/db/docNoAccess", "")
	rest.RequireStatus(t, resp, http.StatusForbidden)
}

func setServerPurgeInterval(t *testing.T, rt *rest.RestTester, newPurgeInterval string) {
	eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	settings := url.Values{}
	settings.Add("purgeInterval", newPurgeInterval)
	settings.Add("autoCompactionDefined", "true")
	settings.Add("parallelDBAndViewCompaction", "false")

	req, err := http.NewRequest("POST", eps[0]+"/pools/default/buckets/"+rt.Bucket().GetName(), strings.NewReader(settings.Encode()))
	require.NoError(t, err)
	req.SetBasicAuth(base.TestClusterUsername(), base.TestClusterPassword())
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := httpClient.Do(req)
	require.NoError(t, err)

	require.Equal(t, resp.StatusCode, http.StatusOK)
}

func TestTombstoneCompactionPurgeInterval(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Server compaction metadata purge interval can only be changed on Couchbase Server")
	}
	testCases := []struct {
		name                              string
		newServerInterval                 string
		dbPurgeInterval                   time.Duration
		expectedPurgeIntervalAfterCompact time.Duration
	}{
		{
			name:                              "Default purge interval updated after db creation",
			newServerInterval:                 "1",
			dbPurgeInterval:                   db.DefaultPurgeInterval,
			expectedPurgeIntervalAfterCompact: time.Hour * 24,
		},
		{
			name:                              "Ignore server interval",
			newServerInterval:                 "1",
			dbPurgeInterval:                   0,
			expectedPurgeIntervalAfterCompact: 0,
		},
		{
			name:                              "Purge interval updated after db creation",
			newServerInterval:                 "1",
			dbPurgeInterval:                   time.Hour,
			expectedPurgeIntervalAfterCompact: time.Hour * 24,
		},
	}
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()
	dbc := rt.GetDatabase()
	ctx := rt.Context()

	cbStore, _ := base.AsCouchbaseStore(rt.Bucket())
	serverPurgeInterval, err := cbStore.MetadataPurgeInterval()
	require.NoError(t, err)
	// Set server purge interval back to what it was for bucket reuse
	defer setServerPurgeInterval(t, rt, fmt.Sprintf("%.2f", serverPurgeInterval.Hours()/24))
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			// Set intervals on server and client
			dbc.PurgeInterval = test.dbPurgeInterval
			setServerPurgeInterval(t, rt, test.newServerInterval)

			// Start compact to modify purge interval
			database, _ := db.GetDatabase(dbc, nil)
			_, err = database.Compact(ctx, false, func(purgedDocCount *int) {}, base.NewSafeTerminator())
			require.NoError(t, err)

			// Check purge interval is as expected
			if !base.TestUseXattrs() {
				// Not using xattrs should cause compaction to not run therefore not changing purge interval
				assert.EqualValues(t, test.dbPurgeInterval, dbc.PurgeInterval)
				return
			}
			assert.EqualValues(t, test.expectedPurgeIntervalAfterCompact, dbc.PurgeInterval)
		})
	}
}

// CBG-2150: Tests that resync status is cluster aware
func TestResyncPersistence(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	tb := base.GetTestBucket(t)
	noCloseTB := tb.NoCloseClone()

	rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: noCloseTB,
	})

	rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb,
	})

	defer rt2.Close()
	defer rt1.Close()

	// Create a document to process through resync
	rt1.CreateDoc(t, "doc1")

	// Start resync
	resp := rt1.SendAdminRequest("POST", "/db/_offline", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	rest.WaitAndAssertCondition(t, func() bool {
		state := atomic.LoadUint32(&rt1.GetDatabase().State)
		return state == db.DBOffline
	})

	resp = rt1.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	// Wait for resync to complete
	var resyncManagerStatus db.ResyncManagerResponse
	err := rt1.WaitForCondition(func() bool {
		resp = rt1.SendAdminRequest("GET", "/db/_resync", "")
		err := json.Unmarshal(resp.BodyBytes(), &resyncManagerStatus)
		assert.NoError(t, err)

		if resyncManagerStatus.State == db.BackgroundProcessStateCompleted {
			return true
		} else {
			t.Logf("resyncManagerStatus.State != %v: %v", db.BackgroundProcessStateCompleted, resyncManagerStatus.State)
			return false
		}
	})
	require.NoError(t, err)

	// Check statuses match
	resp2 := rt2.SendAdminRequest("GET", "/db/_resync", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	fmt.Printf("RT1 Resync Status: %s\n", resp.BodyBytes())
	fmt.Printf("RT2 Resync Status: %s\n", resp2.BodyBytes())
	assert.Equal(t, resp.BodyBytes(), resp2.BodyBytes())
}

// Make sure per DB credentials override per bucket credentials
func TestPerDBCredsOverride(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	// Get test bucket
	tb1 := base.GetTestBucket(t)
	defer tb1.Close()

	config := rest.BootstrapStartupConfigForTest(t)
	config.BucketCredentials = map[string]*base.CredentialsConfig{
		tb1.GetName(): {
			Username: base.TestClusterUsername(),
			Password: base.TestClusterPassword(),
		},
	}
	config.DatabaseCredentials = map[string]*base.CredentialsConfig{
		"db": {
			Username: "invalid",
			Password: "invalid",
		},
	}

	ctx := base.TestCtx(t)
	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)

	serverErr := make(chan error, 0)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())

	couchbaseCluster, err := rest.CreateCouchbaseClusterFromStartupConfig(sc.Config, base.PerUseClusterConnections)
	require.NoError(t, err)
	sc.BootstrapContext.Connection = couchbaseCluster

	dbConfig := `{
		"bucket": "` + tb1.GetName() + `",
		"enable_shared_bucket_access": ` + strconv.FormatBool(base.TestUseXattrs()) + `,
		"use_views": ` + strconv.FormatBool(base.TestsDisableGSI()) + `,
		"num_index_replicas": 0
	}`

	res := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/", dbConfig)
	// Make sure request failed as it could authenticate with the bucket
	assert.Equal(t, http.StatusForbidden, res.StatusCode)

	// Allow database to be created sucessfully
	sc.Config.DatabaseCredentials = map[string]*base.CredentialsConfig{}
	res = rest.BootstrapAdminRequest(t, http.MethodPut, "/db/", dbConfig)
	assert.Equal(t, http.StatusCreated, res.StatusCode)

	// Confirm fetch configs causes bucket credentials to be overrode
	sc.Config.DatabaseCredentials = map[string]*base.CredentialsConfig{
		"db": {
			Username: "invalidUsername",
			Password: "invalidPassword",
		},
	}
	configs, err := sc.FetchConfigs(ctx, false)
	require.NoError(t, err)
	require.NotNil(t, configs["db"])
	assert.Equal(t, "invalidUsername", configs["db"].BucketConfig.Username)
	assert.Equal(t, "invalidPassword", configs["db"].BucketConfig.Password)
}