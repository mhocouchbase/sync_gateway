package auth

import (
	"os"
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

func TestMain(m *testing.M) {
	base.TestBucketPool = base.NewTestBucketPool()
	defer base.TestBucketPool.Close()

	os.Exit(m.Run())
}
