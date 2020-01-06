package base

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/couchbase/gocb"
	"github.com/couchbaselabs/walrus"
	"github.com/pkg/errors"
)

const (
	testBucketQuotaMB    = 100
	testBucketNamePrefix = "sg-int-"
	testClusterUsername  = "Administrator"
	testClusterPassword  = "password"

	// Creates this many buckets in the backing store to be pooled for testing.
	defaultBucketPoolSize = 10
	testEnvPoolSize       = "SG_TEST_BUCKET_POOL_SIZE"

	// Prevents reuse and cleanup of buckets used in failed tests for later inspection.
	// When all pooled buckets are in a preserved state, any remaining tests are skipped.
	testEnvPreserve = "SG_TEST_BUCKET_POOL_PRESERVE"

	// Prints detailed logs from the test pooling framework.
	testEnvVerbose = "SG_TEST_BUCKET_POOL_DEBUG"
)

type GocbTestBucketPool struct {
	readyBucketPool    chan *CouchbaseBucketGoCB
	bucketReadierQueue chan *CouchbaseBucketGoCB
	cluster            *gocb.Cluster
	clusterMgr         *gocb.ClusterManager
	ctxCancelFunc      context.CancelFunc
	defaultBucketSpec  BucketSpec

	useGSI bool

	// preserveBuckets can be set to true to prevent removal of a bucket used in a failing test.
	preserveBuckets bool
	// keep track of number of preserved buckets to prevent bucket exhaustion deadlock
	preservedBucketCount uint32

	// Enables test pool logging
	verbose bool
}

// numBuckets returns the configured number of buckets to use in the pool.
func numBuckets() int {
	numBuckets := defaultBucketPoolSize
	if envPoolSize := os.Getenv(testEnvPoolSize); envPoolSize != "" {
		var err error
		numBuckets, err = strconv.Atoi(envPoolSize)
		if err != nil {
			log.Fatalf("Couldn't parse %s: %v", testEnvPoolSize, err)
		}
	}
	return numBuckets
}

func testCluster(server string) *gocb.Cluster {
	cluster, err := gocb.Connect(server)
	if err != nil {
		log.Fatalf("Couldn't connect to %q: %v", server, err)
	}

	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: testClusterUsername,
		Password: testClusterPassword,
	})
	if err != nil {
		log.Fatalf("Couldn't authenticate with %q: %v", server, err)
	}
	return cluster
}

// NewTestBucketPool initializes a new GocbTestBucketPool.
// Set useGSI to false to skip index creation for tests that don't require GSI to speed up bucket readiness.
func NewTestBucketPool(useGSI bool) *GocbTestBucketPool {
	// We can safely skip setup when we want Walrus buckets to be used.
	if !TestUseCouchbaseServer() {
		return nil
	}

	numBuckets := numBuckets()
	// TODO: What about pooling servers too??
	// That way, we can have unlimited buckets available in a single test pool... True horizontal scalability in tests!
	server := UnitTestUrl()
	cluster := testCluster(server)

	// Used to manage cancellation of worker goroutines
	ctx, ctxCancelFunc := context.WithCancel(context.Background())

	preserveBuckets, _ := strconv.ParseBool(os.Getenv(testEnvPreserve))
	verbose, _ := strconv.ParseBool(os.Getenv(testEnvVerbose))

	tbp := GocbTestBucketPool{
		readyBucketPool:    make(chan *CouchbaseBucketGoCB, numBuckets),
		bucketReadierQueue: make(chan *CouchbaseBucketGoCB, numBuckets),
		cluster:            cluster,
		clusterMgr:         cluster.Manager(testClusterUsername, testClusterPassword),
		ctxCancelFunc:      ctxCancelFunc,
		defaultBucketSpec: BucketSpec{
			Server:          server,
			CouchbaseDriver: GoCBCustomSGTranscoder,
			Auth: TestAuthenticator{
				Username: testClusterUsername,
				Password: testClusterPassword,
			},
			UseXattrs: TestUseXattrs(),
		},
		preserveBuckets: preserveBuckets,
		verbose:         verbose,
		useGSI:          useGSI,
	}

	go tbp.bucketReadier(ctx)

	if err := tbp.createTestBuckets(numBuckets); err != nil {
		log.Fatalf("Couldn't create test buckets: %v", err)
	}

	return &tbp
}

func (tbp *GocbTestBucketPool) Logf(ctx context.Context, format string, args ...interface{}) {
	if tbp == nil || !tbp.verbose {
		return
	}

	format = addPrefixes(format, ctx, LevelNone, KeySGTest)
	if colorEnabled() {
		// Green
		format = "\033[0;32m" + format + "\033[0m"
	}

	_, _ = fmt.Fprintf(consoleFOutput, format+"\n", args...)
}

// GetTestBucket returns a bucket to be used during a test.
// The returned teardownFn must be called once the test is done,
// which closes the bucket, readies it for a new test, and releases back into the pool.
func (tbp *GocbTestBucketPool) GetTestBucket(t testing.TB) (b Bucket, teardownFn func()) {

	ctx := testCtx(t)

	// Return a new Walrus bucket when tbp has not been initialized
	if tbp == nil {
		if !UnitTestUrlIsWalrus() {
			tbp.Logf(ctx, "nil TestBucketPool, but not using a Walrus test URL")
			os.Exit(1)
		}

		walrusBucket := walrus.NewBucket(testBucketNamePrefix + GenerateRandomID())
		ctx := bucketCtx(ctx, walrusBucket)
		tbp.Logf(ctx, "Creating new walrus test bucket")

		return walrusBucket, func() {
			tbp.Logf(ctx, "Teardown called - Closing walrus test bucket")
			walrusBucket.Close()
		}
	}

	if atomic.LoadUint32(&tbp.preservedBucketCount) >= uint32(cap(tbp.readyBucketPool)) {
		tbp.Logf(ctx,
			"No more buckets available for testing. All pooled buckets have been preserved by failing tests.")
		t.Skipf("No more buckets available for testing. All pooled buckets have been preserved for failing tests.")
	}

	tbp.Logf(ctx, "Attempting to get test bucket from pool")
	gocbBucket := <-tbp.readyBucketPool
	ctx = bucketCtx(ctx, gocbBucket)
	tbp.Logf(ctx, "Got test bucket from pool")

	return gocbBucket, func() {
		if tbp.preserveBuckets && t.Failed() {
			tbp.Logf(ctx, "Test using bucket failed. Preserving bucket for later inspection")
			atomic.AddUint32(&tbp.preservedBucketCount, 1)
			return
		}

		tbp.Logf(ctx, "Teardown called - Pushing into bucketReadier queue")
		tbp.bucketReadierQueue <- gocbBucket
	}
}

// bucketCtx extends the parent context with a bucket name.
func bucketCtx(parent context.Context, b Bucket) context.Context {
	return bucketNameCtx(parent, b.GetName())
}

// bucketNameCtx extends the parent context with a bucket name.
func bucketNameCtx(parent context.Context, bucketName string) context.Context {
	parentLogCtx, _ := parent.Value(LogContextKey{}).(LogContext)
	newCtx := LogContext{
		TestName:       parentLogCtx.TestName,
		TestBucketName: bucketName,
	}
	return context.WithValue(parent, LogContextKey{}, newCtx)
}

// testCtx creates a log context for the given test.
func testCtx(t testing.TB) context.Context {
	return context.WithValue(context.Background(), LogContextKey{}, LogContext{TestName: t.Name()})
}

// Close cleans up any buckets, and closes the pool.
func (tbp *GocbTestBucketPool) Close() {
	if tbp == nil {
		// noop
		return
	}

	// Cancel async workers
	tbp.ctxCancelFunc()

	if err := tbp.cluster.Close(); err != nil {
		tbp.Logf(context.Background(), "Couldn't close cluster connection: %v", err)
	}
}

// removes any integration test buckets
func (tbp *GocbTestBucketPool) removeOldTestBuckets() error {
	buckets, err := tbp.clusterMgr.GetBuckets()
	if err != nil {
		return errors.Wrap(err, "couldn't retrieve buckets from cluster manager")
	}

	wg := sync.WaitGroup{}

	for _, b := range buckets {
		if strings.HasPrefix(b.Name, testBucketNamePrefix) {
			ctx := bucketNameCtx(context.Background(), b.Name)
			tbp.Logf(ctx, "Removing old test bucket")
			wg.Add(1)

			// Run the RemoveBucket requests concurrently, as it takes a while per bucket.
			go func(b *gocb.BucketSettings) {
				err := tbp.clusterMgr.RemoveBucket(b.Name)
				if err != nil {
					tbp.Logf(ctx, "Error removing old test bucket: %v", err)
				} else {
					tbp.Logf(ctx, "Removed old test bucket")
				}

				wg.Done()
			}(b)
		}
	}

	wg.Wait()

	return nil
}

// creates a new set of integration test buckets and pushes them into the readier queue.
func (tbp *GocbTestBucketPool) createTestBuckets(numBuckets int) error {

	wg := sync.WaitGroup{}

	existingBuckets, err := tbp.clusterMgr.GetBuckets()
	if err != nil {
		return err
	}

	// create required buckets
	for i := 0; i < numBuckets; i++ {
		bucketName := testBucketNamePrefix + strconv.Itoa(i)
		ctx := bucketNameCtx(context.Background(), bucketName)

		var bucketExists bool
		for _, b := range existingBuckets {
			if bucketName == b.Name {
				tbp.Logf(ctx, "Skipping InsertBucket... Bucket already exists")
				bucketExists = true
			}
		}

		wg.Add(1)

		// Bucket creation takes a few seconds for each bucket,
		// so create and wait for readiness concurrently.
		go func(bucketExists bool) {
			if !bucketExists {
				tbp.Logf(ctx, "Creating new test bucket")
				err := tbp.clusterMgr.InsertBucket(&gocb.BucketSettings{
					Name:          bucketName,
					Quota:         testBucketQuotaMB,
					Type:          gocb.Couchbase,
					FlushEnabled:  true,
					IndexReplicas: false,
					Replicas:      0,
				})
				if err != nil {
					tbp.Logf(ctx, "Couldn't create test bucket: %v", err)
					os.Exit(1)
				}

				// Have an initial wait for bucket creation before the OpenBucket retry starts
				// time.Sleep(time.Second * 2 * time.Duration(numBuckets))
			}

			bucketSpec := tbp.defaultBucketSpec
			bucketSpec.BucketName = bucketName

			waitForNewBucketWorker := func() (shouldRetry bool, err error, value interface{}) {
				gocbBucket, err := GetCouchbaseBucketGoCB(bucketSpec)
				if err != nil {
					tbp.Logf(ctx, "Retrying OpenBucket")
					return true, err, nil
				}
				return false, nil, gocbBucket
			}
			err, val := RetryLoop("waitForNewBucket", waitForNewBucketWorker,
				// The more buckets we're creating simultaneously on the cluster,
				// the longer this seems to take, so scale the wait time.
				CreateSleeperFunc(5*numBuckets, 1000))
			if err != nil {
				tbp.Logf(ctx, "Timed out trying to open new bucket: %v", err)
				os.Exit(1)
			}

			gocbBucket := val.(*CouchbaseBucketGoCB)

			tbp.Logf(ctx, "Putting gocbBucket onto bucketReadierQueue")
			tbp.bucketReadierQueue <- gocbBucket

			wg.Done()
		}(bucketExists)
	}

	wg.Wait()

	return nil
}

// bucketReadier takes a channel of "dirty" buckets, and gets them ready for being put back into the pool.
// - flushes if required
// - initializes views
// - initializes indexes
// - pushes them back into the ready bucket pool.
func (tbp *GocbTestBucketPool) bucketReadier(ctx context.Context) {
	tbp.Logf(context.Background(), "Starting bucketReadier")

loop:
	for {
		select {
		case <-ctx.Done():
			tbp.Logf(context.Background(), "bucketReadier got ctx cancelled")
			break loop

		case b := <-tbp.bucketReadierQueue:
			ctx := bucketCtx(ctx, b)
			tbp.Logf(ctx, "bucketReadier got bucket")

			go func(b Bucket) {
				gocbBucket, ok := AsGoCBBucket(b)
				if !ok {
					panic(fmt.Sprintf("not a gocb bucket: %T", b))
				}

				// Empty bucket
				if itemCount, err := gocbBucket.QueryBucketItemCount(); err != nil {
					panic(err)
				} else if itemCount == 0 {
					tbp.Logf(ctx, "Bucket already empty - skipping flush")
				} else {
					tbp.Logf(ctx, "Bucket not empty (%d items), flushing bucket", itemCount)
					err := gocbBucket.Flush()
					if err != nil {
						panic(err)
					}
				}

				// Initialize Views
				// FIXME: Import cycle (base -> db -> base)
				// if err := db.InitializeViews(gocbBucket); err != nil {
				// 	panic(err)
				// }

				// Initialize Indexes
				// FIXME: Import cycle (base -> db -> base)
				// if tbp.useGSI {
				// 	// init GSI indexes
				// 	err := db.InitializeIndexes(gocbBucket, TestUseXattrs(), 0)
				// 	if err != nil {
				// 		panic(err)
				// 	}
				//
				// 	// Since GetTestBucket() always returns an _empty_ bucket, it's safe to wait for the indexes to be empty
				// 	err = db.WaitForIndexEmpty(gocbBucket, TestUseXattrs())
				// 	if err != nil {
				// 		tbp.Logf(ctx, "Bucket %q error WaitForIndexEmpty: %v.  Drop indexes and retry", b.GetName(), err)
				// 		if err := DropAllBucketIndexes(gocbBucket); err != nil {
				// 			panic(err)
				// 		}
				// 	}
				// }

				tbp.Logf(ctx, "Bucket ready, putting back into ready pool")
				tbp.readyBucketPool <- gocbBucket
			}(b)
		}
	}

	tbp.Logf(context.Background(), "Stopping bucketReadier")
}
