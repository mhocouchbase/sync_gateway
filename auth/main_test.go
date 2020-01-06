package auth

import (
	"os"
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

func TestMain(m *testing.M) {
	// auth tests don't require GSI
	base.TestBucketPool = base.NewTestBucketPool(false)
	defer base.TestBucketPool.Close()

	os.Exit(m.Run())
}
