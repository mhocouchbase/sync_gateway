package base

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	TestBucketPool = NewTestBucketPool()
	defer TestBucketPool.Close()

	os.Exit(m.Run())
}
