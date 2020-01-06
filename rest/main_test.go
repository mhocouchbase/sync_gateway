package rest

import (
	"os"
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

func TestMain(m *testing.M) {
	// rest tests require GSI
	base.TestBucketPool = base.NewTestBucketPool(true)
	defer base.TestBucketPool.Close()

	os.Exit(m.Run())
}
