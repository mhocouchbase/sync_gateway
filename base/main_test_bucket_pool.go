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
	"time"

	"github.com/couchbase/gocb"
	"github.com/couchbaselabs/walrus"
	"github.com/pkg/errors"
)

// GTestBucketPool is a global instance of a TestBucketPool used to manage a pool of buckets for integration testing.
var GTestBucketPool *TestBucketPool

const (
	tbpEnvClusterUsername     = "SG_TEST_USERNAME"
	tbpDefaultClusterUsername = DefaultCouchbaseAdministrator
	tbpEnvClusterPassword     = "SG_TEST_PASSWORD"
	tbpDefaultClusterPassword = DefaultCouchbasePassword

	tbpBucketNamePrefix = "sg_int_"

	// Creates this many buckets in the backing store to be pooled for testing.
	tbpDefaultBucketPoolSize = 3
	tbpEnvPoolSize           = "SG_TEST_BUCKET_POOL_SIZE"

	defaultBucketQuotaMB = 150
	tbpEnvBucketQuotaMB  = "SG_TEST_BUCKET_QUOTA_MB"

	// Prevents reuse and cleanup of buckets used in failed tests for later inspection.
	// When all pooled buckets are in a preserved state, any remaining tests are skipped instead of blocking waiting for a bucket.
	tbpEnvPreserve = "SG_TEST_BUCKET_POOL_PRESERVE"

	// When set to true, all existing test buckets are removed and recreated, instead of running the bucket readier.
	tbpEnvRecreate = "SG_TEST_BACKING_STORE_RECREATE"

	// Prints detailed debug logs from the test pooling framework.
	tbpEnvVerbose = "SG_TEST_BUCKET_POOL_DEBUG"
)

// TestBucketPool is used to manage a pool of gocb buckets on a Couchbase Server for testing purposes.
// The zero-value/uninitialized version of this struct is safe to use as Walrus buckets are returned.
type TestBucketPool struct {
	// integrationMode should be true if using Couchbase Server. If this is false, Walrus buckets are returned instead of pooled buckets.
	integrationMode bool

	readyBucketPool        chan *CouchbaseBucketGoCB
	bucketReadierQueue     chan tbpBucketName
	bucketReadierWaitGroup *sync.WaitGroup
	cluster                *gocb.Cluster
	clusterMgr             *gocb.ClusterManager
	ctxCancelFunc          context.CancelFunc
	defaultBucketSpec      BucketSpec

	bucketInitFunc TBPBucketInitFunc

	stats bucketPoolStats

	// preserveBuckets can be set to true to prevent removal of a bucket used in a failing test.
	preserveBuckets bool
	// preservedBucketCount keeps track of number of preserved buckets to prevent bucket exhaustion deadlock.
	preservedBucketCount uint32

	// verbose flag controls debug test pool logging.
	verbose AtomicBool
}

// NewTestBucketPool initializes a new TestBucketPool. To be called from TestMain for packages requiring test buckets.
func NewTestBucketPool(bucketReadierFunc TBPBucketReadierFunc, bucketInitFunc TBPBucketInitFunc) *TestBucketPool {
	// We can safely skip setup when we want Walrus buckets to be used. They'll be created on-demand via GetTestBucketAndSpec.
	if !TestUseCouchbaseServer() {
		tbp := TestBucketPool{
			bucketInitFunc: bucketInitFunc,
		}
		tbp.verbose.Set(tbpVerbose())
		return &tbp
	}

	_, err := SetMaxFileDescriptors(5000)
	if err != nil {
		panic(err)
	}

	numBuckets := tbpNumBuckets()
	// TODO: What about pooling servers too??
	// That way, we can have unlimited buckets available in a single test pool... True horizontal scalability in tests!
	cluster := tbpCluster(UnitTestUrl())

	// Used to manage cancellation of worker goroutines
	ctx, ctxCancelFunc := context.WithCancel(context.Background())

	preserveBuckets, _ := strconv.ParseBool(os.Getenv(tbpEnvPreserve))

	tbp := TestBucketPool{
		integrationMode:        true,
		readyBucketPool:        make(chan *CouchbaseBucketGoCB, numBuckets),
		bucketReadierQueue:     make(chan tbpBucketName, numBuckets),
		bucketReadierWaitGroup: &sync.WaitGroup{},
		cluster:                cluster,
		clusterMgr:             cluster.Manager(tbpClusterUsername(), tbpClusterPassword()),
		ctxCancelFunc:          ctxCancelFunc,
		defaultBucketSpec:      tbpDefaultBucketSpec,
		preserveBuckets:        preserveBuckets,
		bucketInitFunc:         bucketInitFunc,
	}

	tbp.verbose.Set(tbpVerbose())

	// Start up an async readier worker to process dirty buckets
	go tbp.bucketReadierWorker(ctx, bucketReadierFunc)

	// Remove old test buckets (if desired)
	removeOldBuckets, _ := strconv.ParseBool(os.Getenv(tbpEnvRecreate))
	if removeOldBuckets {
		err := tbp.removeOldTestBuckets()
		if err != nil {
			log.Fatalf("Couldn't remove old test buckets: %v", err)
		}
	}

	// Make sure the test buckets are created and put into the readier worker queue
	start := time.Now()
	if err := tbp.createTestBuckets(numBuckets, tbpBucketQuotaMB(), bucketInitFunc); err != nil {
		log.Fatalf("Couldn't create test buckets: %v", err)
	}
	atomic.AddInt32(&tbp.stats.TotalBucketInitCount, int32(numBuckets))
	atomic.AddInt64(&tbp.stats.TotalBucketInitDurationNano, time.Since(start).Nanoseconds())

	return &tbp
}

// Logf formats the given test bucket logging and logs to stderr.
func (tbp *TestBucketPool) Logf(ctx context.Context, format string, args ...interface{}) {
	if tbp != nil && !tbp.verbose.IsTrue() {
		return
	}

	format = addPrefixes(format, ctx, LevelNone, KeySGTest)
	if colorEnabled() {
		// Green
		format = "\033[0;32m" + format + "\033[0m"
	}

	_, _ = fmt.Fprintf(consoleFOutput, format+"\n", args...)
}

// GetTestBucketAndSpec returns a bucket to be used during a test.
// The returned teardownFn MUST be called once the test is done,
// which closes the bucket, readies it for a new test, and releases back into the pool.
func (tbp *TestBucketPool) GetTestBucketAndSpec(t testing.TB) (b Bucket, s BucketSpec, teardownFn func()) {

	ctx := testCtx(t)

	// Return a new Walrus bucket when tbp has not been initialized
	if !tbp.integrationMode {
		if !UnitTestUrlIsWalrus() {
			tbp.Logf(ctx, "nil TestBucketPool, but not using a Walrus test URL")
			os.Exit(1)
		}

		walrusBucket := walrus.NewBucket(tbpBucketNamePrefix + "walrus_" + GenerateRandomID())

		// Wrap Walrus buckets with a leaky bucket to support vbucket IDs on feed.
		b = &LeakyBucket{bucket: walrusBucket, config: LeakyBucketConfig{TapFeedVbuckets: true}}

		ctx := bucketCtx(ctx, b)
		tbp.Logf(ctx, "Creating new walrus test bucket")

		initFuncStart := time.Now()
		err := tbp.bucketInitFunc(ctx, b, tbp)
		if err != nil {
			panic(err)
		}
		atomic.AddInt32(&tbp.stats.TotalBucketInitCount, 1)
		atomic.AddInt64(&tbp.stats.TotalBucketInitDurationNano, time.Since(initFuncStart).Nanoseconds())

		atomic.AddInt32(&tbp.stats.NumBucketsOpened, 1)
		openedStart := time.Now()
		return b, getBucketSpec(tbpBucketName(b.GetName())), func() {
			atomic.AddInt32(&tbp.stats.NumBucketsClosed, 1)
			atomic.AddInt64(&tbp.stats.TotalInuseBucketNano, time.Since(openedStart).Nanoseconds())
			tbp.Logf(ctx, "Teardown called - Closing walrus test bucket")
			b.Close()
		}
	}

	if atomic.LoadUint32(&tbp.preservedBucketCount) >= uint32(cap(tbp.readyBucketPool)) {
		tbp.Logf(ctx,
			"No more buckets available for testing. All pooled buckets have been preserved by failing tests.")
		t.Skipf("No more buckets available for testing. All pooled buckets have been preserved for failing tests.")
	}

	tbp.Logf(ctx, "Attempting to get test bucket from pool")
	waitingBucketStart := time.Now()
	gocbBucket := <-tbp.readyBucketPool
	atomic.AddInt64(&tbp.stats.TotalWaitingForReadyBucketNano, time.Since(waitingBucketStart).Nanoseconds())
	ctx = bucketCtx(ctx, gocbBucket)
	tbp.Logf(ctx, "Got test bucket from pool")

	atomic.AddInt32(&tbp.stats.NumBucketsOpened, 1)
	bucketOpenStart := time.Now()
	return gocbBucket, getBucketSpec(tbpBucketName(gocbBucket.GetName())), func() {
		atomic.AddInt32(&tbp.stats.NumBucketsClosed, 1)
		atomic.AddInt64(&tbp.stats.TotalInuseBucketNano, time.Since(bucketOpenStart).Nanoseconds())
		tbp.Logf(ctx, "Teardown called - closing bucket")
		gocbBucket.Close()

		if tbp.preserveBuckets && t.Failed() {
			tbp.Logf(ctx, "Test using bucket failed. Preserving bucket for later inspection")
			atomic.AddUint32(&tbp.preservedBucketCount, 1)
			return
		}

		tbp.Logf(ctx, "Teardown called - Pushing into bucketReadier queue")
		tbp.addBucketToReadierQueue(ctx, tbpBucketName(gocbBucket.GetName()))
	}
}

func (tbp *TestBucketPool) addBucketToReadierQueue(ctx context.Context, name tbpBucketName) {
	tbp.bucketReadierWaitGroup.Add(1)
	tbp.Logf(ctx, "Putting bucket onto bucketReadierQueue")
	tbp.bucketReadierQueue <- name
}

// Close waits for any buckets to be cleaned, and closes the pool.
func (tbp *TestBucketPool) Close() {
	if tbp == nil {
		// noop
		return
	}

	// Cancel async workers
	if tbp.ctxCancelFunc != nil {
		tbp.bucketReadierWaitGroup.Wait()
		tbp.ctxCancelFunc()
	}

	if tbp.cluster != nil {
		if err := tbp.cluster.Close(); err != nil {
			tbp.Logf(context.Background(), "Couldn't close cluster connection: %v", err)
		}
	}

	tbp.printStats()
}

// printStats outputs test bucket stats for the current package's test run.
func (tbp *TestBucketPool) printStats() {

	numBucketsOpened := time.Duration(atomic.LoadInt32(&tbp.stats.NumBucketsOpened))
	if numBucketsOpened == 0 {
		// we may have been running benchmarks if we've opened zero test buckets
		// in any case; if we have no stats, don't bother printing anything.
		return
	}

	totalBucketInitTime := time.Duration(atomic.LoadInt64(&tbp.stats.TotalBucketInitDurationNano))
	totalBucketInitCount := time.Duration(atomic.LoadInt32(&tbp.stats.TotalBucketInitCount))

	totalBucketReadierTime := time.Duration(atomic.LoadInt64(&tbp.stats.TotalBucketReadierDurationNano))
	totalBucketReadierCount := time.Duration(atomic.LoadInt32(&tbp.stats.TotalBucketReadierCount))

	totalBucketWaitTime := time.Duration(atomic.LoadInt64(&tbp.stats.TotalWaitingForReadyBucketNano))

	totalBucketUseTime := time.Duration(atomic.LoadInt64(&tbp.stats.TotalInuseBucketNano))

	origVerbose := tbp.verbose.IsTrue()
	tbp.verbose.Set(true)
	ctx := context.Background()

	tbp.Logf(ctx, "==========================")
	tbp.Logf(ctx, "= Test Bucket Pool Stats =")
	tbp.Logf(ctx, "==========================")
	if totalBucketInitCount > 0 {
		tbp.Logf(ctx, "Total bucket init time: %s for %d buckets (avg: %s)", totalBucketInitTime, totalBucketInitCount, totalBucketInitTime/totalBucketInitCount)
	} else {
		tbp.Logf(ctx, "Total bucket init time: %s for %d buckets", totalBucketInitTime, totalBucketInitCount)
	}
	if totalBucketReadierCount > 0 {
		tbp.Logf(ctx, "Total bucket readier time: %s for %d buckets (avg: %s)", totalBucketReadierTime, totalBucketReadierCount, totalBucketReadierTime/totalBucketReadierCount)
	} else {
		tbp.Logf(ctx, "Total bucket readier time: %s for %d buckets", totalBucketReadierTime, totalBucketReadierCount)
	}
	tbp.Logf(ctx, "Total buckets opened/closed: %d/%d", numBucketsOpened, atomic.LoadInt32(&tbp.stats.NumBucketsClosed))
	if numBucketsOpened > 0 {
		tbp.Logf(ctx, "Total time waiting for ready bucket: %s over %d buckets (avg: %s)", totalBucketWaitTime, numBucketsOpened, totalBucketWaitTime/numBucketsOpened)
		tbp.Logf(ctx, "Total time tests using buckets: %s (avg: %s)", totalBucketUseTime, totalBucketUseTime/numBucketsOpened)
	} else {
		tbp.Logf(ctx, "Total time waiting for ready bucket: %s over %d buckets", totalBucketWaitTime, numBucketsOpened)
		tbp.Logf(ctx, "Total time tests using buckets: %s", totalBucketUseTime)
	}
	tbp.Logf(ctx, "==========================")

	tbp.verbose.Set(origVerbose)
}

// removeOldTestBuckets removes all buckets starting with testBucketNamePrefix
func (tbp *TestBucketPool) removeOldTestBuckets() error {
	buckets, err := getBuckets(tbp.clusterMgr)
	if err != nil {
		return errors.Wrap(err, "couldn't retrieve buckets from cluster manager")
	}

	wg := sync.WaitGroup{}

	for _, b := range buckets {
		if strings.HasPrefix(b.Name, tbpBucketNamePrefix) {
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

// getBuckets returns a list of buckets in the cluster.
func getBuckets(cm *gocb.ClusterManager) ([]*gocb.BucketSettings, error) {
	buckets, err := cm.GetBuckets()
	if err != nil {
		// special handling for gocb's empty non-nil error if we send this request with invalid credentials
		if err.Error() == "" {
			err = errors.New("couldn't get buckets from cluster, check authentication credentials")
		}
		return nil, err
	}
	return buckets, nil
}

// createTestBuckets creates a new set of integration test buckets and pushes them into the readier queue.
func (tbp *TestBucketPool) createTestBuckets(numBuckets int, bucketQuotaMB int, bucketInitFunc TBPBucketInitFunc) error {

	// get a list of any existing buckets, so we can skip creation of them.
	existingBuckets, err := getBuckets(tbp.clusterMgr)
	if err != nil {
		return err
	}

	// keep references to opened buckets for use later in this function
	openBuckets := make([]*CouchbaseBucketGoCB, numBuckets)

	wg := sync.WaitGroup{}
	wg.Add(numBuckets)

	// create required number of buckets (skipping any already existing ones)
	for i := 0; i < numBuckets; i++ {
		testBucketName := tbpBucketNamePrefix + strconv.Itoa(i)
		ctx := bucketNameCtx(context.Background(), testBucketName)

		var bucketExists bool
		for _, b := range existingBuckets {
			if testBucketName == b.Name {
				tbp.Logf(ctx, "Skipping InsertBucket... Bucket already exists")
				bucketExists = true
			}
		}

		// Bucket creation takes a few seconds for each bucket,
		// so create and wait for readiness concurrently.
		go func(i int, bucketExists bool) {
			if !bucketExists {
				tbp.Logf(ctx, "Creating new test bucket")
				err := tbp.clusterMgr.InsertBucket(&gocb.BucketSettings{
					Name:          testBucketName,
					Quota:         bucketQuotaMB,
					Type:          gocb.Couchbase,
					FlushEnabled:  true,
					IndexReplicas: false,
					Replicas:      0,
				})
				if err != nil {
					tbp.Logf(ctx, "Couldn't create test bucket: %v", err)
					os.Exit(1)
				}
			}

			b, err := tbp.openTestBucket(tbpBucketName(testBucketName), CreateSleeperFunc(5*numBuckets, 1000))
			if err != nil {
				tbp.Logf(ctx, "Timed out trying to open new bucket: %v", err)
				os.Exit(1)
			}
			openBuckets[i] = b

			wg.Done()
		}(i, bucketExists)
	}

	// wait for the async bucket creation and opening of buckets to finish
	wg.Wait()

	// All the buckets are created and opened, so now we can perform some synchronous setup (e.g. Creating GSI indexes)
	for i := 0; i < numBuckets; i++ {
		testBucketName := tbpBucketNamePrefix + strconv.Itoa(i)
		ctx := bucketNameCtx(context.Background(), testBucketName)

		tbp.Logf(ctx, "running bucketInitFunc")
		b := openBuckets[i]

		if err, _ := RetryLoop(b.GetName()+"bucketInitRetry", func() (bool, error, interface{}) {
			tbp.Logf(ctx, "Running bucket through init function")
			err = bucketInitFunc(ctx, b, tbp)
			if err != nil {
				tbp.Logf(ctx, "Couldn't init bucket, got error: %v - Retrying", err)
				return true, err, nil
			}
			return false, nil, nil
		}, CreateSleeperFunc(5, 1000)); err != nil {
			tbp.Logf(ctx, "Couldn't init bucket, got error: %v - Aborting", err)
			os.Exit(1)
		}

		b.Close()
		tbp.addBucketToReadierQueue(ctx, tbpBucketName(testBucketName))
	}

	return nil
}

// bucketReadierWorker reads a channel of "dirty" buckets (bucketReadierQueue), does something to get them ready, and then puts them back into the pool.
// The mechanism for getting the bucket ready can vary by package being tested (for instance, a package not requiring views or GSI can use FlushBucketEmptierFunc)
// A package requiring views or GSI, will need to pass in the db.ViewsAndGSIBucketReadier function.
func (tbp *TestBucketPool) bucketReadierWorker(ctx context.Context, bucketReadierFunc TBPBucketReadierFunc) {
	tbp.Logf(context.Background(), "Starting bucketReadier")

loop:
	for {
		select {
		case <-ctx.Done():
			tbp.Logf(context.Background(), "bucketReadier got ctx cancelled")
			break loop

		case testBucketName := <-tbp.bucketReadierQueue:
			atomic.AddInt32(&tbp.stats.TotalBucketReadierCount, 1)
			ctx := bucketNameCtx(ctx, string(testBucketName))
			tbp.Logf(ctx, "bucketReadier got bucket")

			go func(testBucketName tbpBucketName) {
				// We might not actually be "done" with the bucket if something fails,
				// but we need to release the waitgroup so tbp.Close() doesn't block forever.
				defer tbp.bucketReadierWaitGroup.Done()

				start := time.Now()
				b, err := tbp.openTestBucket(testBucketName, CreateSleeperFunc(5, 1000))
				if err != nil {
					tbp.Logf(ctx, "Couldn't open bucket to get ready, got error: %v", err)
					return
				}

				err, _ = RetryLoop(b.GetName()+"bucketReadierRetry", func() (bool, error, interface{}) {
					tbp.Logf(ctx, "Running bucket through readier function")
					err = bucketReadierFunc(ctx, b, tbp)
					if err != nil {
						tbp.Logf(ctx, "Couldn't ready bucket, got error: %v - Retrying", err)
						return true, err, nil
					}
					return false, nil, nil
				}, CreateSleeperFunc(5, 1000))
				if err != nil {
					tbp.Logf(ctx, "Couldn't ready bucket, got error: %v - Aborting readier for bucket", err)
					return
				}

				tbp.Logf(ctx, "Bucket ready, putting back into ready pool")
				tbp.readyBucketPool <- b
				atomic.AddInt64(&tbp.stats.TotalBucketReadierDurationNano, time.Since(start).Nanoseconds())
			}(testBucketName)
		}
	}

	tbp.Logf(context.Background(), "Stopped bucketReadier")
}

// openTestBucket opens the bucket of the given name for the gocb cluster in the given TestBucketPool.
func (tbp *TestBucketPool) openTestBucket(testBucketName tbpBucketName, sleeper RetrySleeper) (*CouchbaseBucketGoCB, error) {

	ctx := bucketNameCtx(context.Background(), string(testBucketName))

	bucketSpec := tbp.defaultBucketSpec
	bucketSpec.BucketName = string(testBucketName)

	waitForNewBucketWorker := func() (shouldRetry bool, err error, value interface{}) {
		gocbBucket, err := GetCouchbaseBucketGoCBFromAuthenticatedCluster(tbp.cluster, bucketSpec, "")
		if err != nil {
			tbp.Logf(ctx, "Retrying OpenBucket")
			return true, err, nil
		}
		return false, nil, gocbBucket
	}

	tbp.Logf(ctx, "Opening bucket")
	err, val := RetryLoop("waitForNewBucket", waitForNewBucketWorker, sleeper)

	gocbBucket, _ := val.(*CouchbaseBucketGoCB)

	return gocbBucket, err
}

// TBPBucketInitFunc is a function that is run once (synchronously) when creating/opening a bucket.
type TBPBucketInitFunc func(ctx context.Context, b Bucket, tbp *TestBucketPool) error

// NoopInitFunc does nothing to init a bucket. This can be used in conjunction with FlushBucketReadier when there's no requirement for views/GSI.
var NoopInitFunc TBPBucketInitFunc = func(ctx context.Context, b Bucket, tbp *TestBucketPool) error {
	return nil
}

// PrimaryIndexInitFunc creates a primary index on the given bucket. This can then be used with N1QLBucketEmptierFunc, for improved compatibility with GSI.
// Will be used when GSI is re-enabled (CBG-813)
var PrimaryIndexInitFunc TBPBucketInitFunc = func(ctx context.Context, b Bucket, tbp *TestBucketPool) error {
	gocbBucket, ok := AsGoCBBucket(b)
	if !ok {
		tbp.Logf(ctx, "skipping primary index creation for non-gocb bucket")
		return nil
	}

	if hasPrimary, _, err := gocbBucket.getIndexMetaWithoutRetry(PrimaryIndexName); err != nil {
		return err
	} else if !hasPrimary {
		err := gocbBucket.CreatePrimaryIndex(PrimaryIndexName, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// TBPBucketReadierFunc is a function that runs once a test is finished with a bucket. This runs asynchronously.
type TBPBucketReadierFunc func(ctx context.Context, b *CouchbaseBucketGoCB, tbp *TestBucketPool) error

// FlushBucketEmptierFunc ensures the bucket is empty by flushing. It is not recommended to use with GSI.
var FlushBucketEmptierFunc TBPBucketReadierFunc = func(ctx context.Context, b *CouchbaseBucketGoCB, tbp *TestBucketPool) error {
	return b.Flush()
}

// N1QLBucketEmptierFunc ensures the bucket is empty by using N1QL deletes. This is the preferred approach when using GSI.
// Will be used when GSI is re-enabled (CBG-813)
var N1QLBucketEmptierFunc TBPBucketReadierFunc = func(ctx context.Context, b *CouchbaseBucketGoCB, tbp *TestBucketPool) error {
	if hasPrimary, _, err := b.getIndexMetaWithoutRetry(PrimaryIndexName); err != nil {
		return err
	} else if !hasPrimary {
		return fmt.Errorf("bucket does not have primary index, so can't empty bucket using N1QL")
	}

	if itemCount, err := b.QueryBucketItemCount(); err != nil {
		return err
	} else if itemCount == 0 {
		tbp.Logf(ctx, "Bucket already empty - skipping")
	} else {
		tbp.Logf(ctx, "Bucket not empty (%d items), emptying bucket via N1QL", itemCount)
		// Use N1QL to empty bucket, with the hope that the query service is happier to deal with this than a bucket flush/rollback.
		// Requires a primary index on the bucket.
		res, err := b.Query(`DELETE FROM $_bucket`, nil, gocb.RequestPlus, false)
		if err != nil {
			return err
		}
		_ = res.Close()
	}

	return nil
}

// bucketPoolStats is the struct used to track runtime/counts of various test bucket operations.
// printStats() is called once a package's tests have finished to output these stats.
type bucketPoolStats struct {
	TotalBucketInitDurationNano    int64
	TotalBucketInitCount           int32
	TotalBucketReadierDurationNano int64
	TotalBucketReadierCount        int32
	NumBucketsOpened               int32
	NumBucketsClosed               int32
	TotalWaitingForReadyBucketNano int64
	TotalInuseBucketNano           int64
}

// tbpBucketName use a strongly typed bucket name.
type tbpBucketName string

// tbpCluster returns an authenticated gocb Cluster for the given server URL.
func tbpCluster(server string) *gocb.Cluster {
	spec := BucketSpec{
		Server: server,
	}

	connStr, err := spec.GetGoCBConnString()
	if err != nil {
		log.Fatalf("error getting connection string: %v", err)
	}

	cluster, err := gocb.Connect(connStr)
	if err != nil {
		log.Fatalf("Couldn't connect to %q: %v", server, err)
	}

	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: tbpClusterUsername(),
		Password: tbpClusterPassword(),
	})
	if err != nil {
		log.Fatalf("Couldn't authenticate with %q: %v", server, err)
	}

	return cluster
}

var tbpDefaultBucketSpec = BucketSpec{
	Server:          UnitTestUrl(),
	CouchbaseDriver: GoCBCustomSGTranscoder,
	Auth: TestAuthenticator{
		Username: tbpClusterUsername(),
		Password: tbpClusterPassword(),
	},
	UseXattrs: TestUseXattrs(),
}

// getBucketSpec returns a new BucketSpec for the given bucket name.
func getBucketSpec(testBucketName tbpBucketName) BucketSpec {
	bucketSpec := tbpDefaultBucketSpec
	bucketSpec.BucketName = string(testBucketName)
	return bucketSpec
}

// tbpNumBuckets returns the configured number of buckets to use in the pool.
func tbpNumBuckets() int {
	numBuckets := tbpDefaultBucketPoolSize
	if envPoolSize := os.Getenv(tbpEnvPoolSize); envPoolSize != "" {
		var err error
		numBuckets, err = strconv.Atoi(envPoolSize)
		if err != nil {
			log.Fatalf("Couldn't parse %s: %v", tbpEnvPoolSize, err)
		}
	}
	return numBuckets
}

// tbpBucketQuotaMB returns the configured bucket RAM quota.
func tbpBucketQuotaMB() int {
	bucketQuota := defaultBucketQuotaMB
	if envBucketQuotaMB := os.Getenv(tbpEnvBucketQuotaMB); envBucketQuotaMB != "" {
		var err error
		bucketQuota, err = strconv.Atoi(envBucketQuotaMB)
		if err != nil {
			log.Fatalf("Couldn't parse %s: %v", tbpEnvBucketQuotaMB, err)
		}
	}
	return bucketQuota
}

// tbpVerbose returns the configured test bucket pool verbose flag.
func tbpVerbose() bool {
	verbose, _ := strconv.ParseBool(os.Getenv(tbpEnvVerbose))
	return verbose
}

// tbpClusterUsername returns the configured cluster username.
func tbpClusterUsername() string {
	username := tbpDefaultClusterUsername
	if envClusterUsername := os.Getenv(tbpEnvClusterUsername); envClusterUsername != "" {
		username = envClusterUsername
	}
	return username
}

// tbpClusterPassword returns the configured cluster password.
func tbpClusterPassword() string {
	password := tbpDefaultClusterPassword
	if envClusterPassword := os.Getenv(tbpEnvClusterPassword); envClusterPassword != "" {
		password = envClusterPassword
	}
	return password
}