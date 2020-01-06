package base

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// base tests require GSI
	TestBucketPool = NewTestBucketPool(true)
	defer TestBucketPool.Close()

	os.Exit(m.Run())
}
