package lib

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func testResponse(req *http.Request, status int, headers http.Header) *http.Response {
	if headers == nil {
		headers = make(http.Header)
	}
	return &http.Response{
		Status:        http.StatusText(status),
		StatusCode:    status,
		Header:        headers,
		Body:          io.NopCloser(strings.NewReader("ok")),
		ContentLength: 2,
		Request:       req,
	}
}

func scheduledRequest(ctx context.Context, state *clientState, bucket *bucketState, path string) *http.Request {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://discord.com/api/v10"+path, nil)
	meta := &requestMetadata{state: state, bucket: bucket, bucketPath: path, metricsPath: path}
	return req.WithContext(context.WithValue(req.Context(), requestMetadataKey, meta))
}

func TestBucketWaitHonorsCancellation(t *testing.T) {
	bucket := newBucketState()
	bucket.readyAt = time.Now().Add(time.Hour)
	state := &clientState{buckets: map[uint64]*bucketState{}, identifier: "NoAuth", queueType: NoAuth}
	var calls atomic.Int64
	transport := &scheduledTransport{
		manager: &QueueManager{clusterGlobalRateLimiter: NewClusterGlobalRateLimiter()},
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			return testResponse(req, http.StatusOK, nil), nil
		}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := transport.RoundTrip(scheduledRequest(ctx, state, bucket, "/gateway"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if calls.Load() != 0 || bucket.active.Load() != 0 {
		t.Fatalf("canceled request leaked through: calls=%d active=%d", calls.Load(), bucket.active.Load())
	}
}

func TestBucketHeldUntilBodyClose(t *testing.T) {
	bucket := newBucketState()
	state := &clientState{buckets: map[uint64]*bucketState{}, identifier: "NoAuth", queueType: NoAuth}
	var calls atomic.Int64
	transport := &scheduledTransport{
		manager: &QueueManager{clusterGlobalRateLimiter: NewClusterGlobalRateLimiter()},
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			return testResponse(req, http.StatusOK, nil), nil
		}),
	}

	first, err := transport.RoundTrip(scheduledRequest(context.Background(), state, bucket, "/gateway"))
	if err != nil {
		t.Fatal(err)
	}
	secondDone := make(chan *http.Response, 1)
	go func() {
		resp, _ := transport.RoundTrip(scheduledRequest(context.Background(), state, bucket, "/gateway"))
		secondDone <- resp
	}()

	select {
	case <-secondDone:
		t.Fatal("second request passed the bucket before the first response closed")
	case <-time.After(20 * time.Millisecond):
	}
	if calls.Load() != 1 {
		t.Fatalf("expected one outbound call, got %d", calls.Load())
	}
	_ = first.Body.Close()
	select {
	case second := <-secondDone:
		_ = second.Body.Close()
	case <-time.After(time.Second):
		t.Fatal("second request did not resume after body close")
	}
}

func TestBucketGateIsFIFO(t *testing.T) {
	bucket := newBucketState()
	if err := bucket.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	const waiterCount = 8
	order := make(chan int, waiterCount)
	for i := range waiterCount {
		go func(i int) {
			if err := bucket.acquire(context.Background()); err != nil {
				return
			}
			order <- i
			bucket.release()
		}(i)
		deadline := time.Now().Add(time.Second)
		for {
			bucket.gate.mu.Lock()
			queued := len(bucket.gate.waiters)
			bucket.gate.mu.Unlock()
			if queued == i+1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("waiter did not enter FIFO queue")
			}
		}
	}
	bucket.release()
	for want := range waiterCount {
		if got := <-order; got != want {
			t.Fatalf("FIFO order changed: got %d, want %d", got, want)
		}
	}
}

func TestDifferentBucketsRunConcurrently(t *testing.T) {
	state := &clientState{buckets: map[uint64]*bucketState{}, identifier: "NoAuth", queueType: NoAuth}
	firstBucket, secondBucket := newBucketState(), newBucketState()
	var calls atomic.Int64
	transport := &scheduledTransport{
		manager: &QueueManager{clusterGlobalRateLimiter: NewClusterGlobalRateLimiter()},
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			return testResponse(req, http.StatusOK, nil), nil
		}),
	}

	first, err := transport.RoundTrip(scheduledRequest(context.Background(), state, firstBucket, "/gateway"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := transport.RoundTrip(scheduledRequest(context.Background(), state, secondBucket, "/users/@me"))
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected two concurrent outbound calls, got %d", calls.Load())
	}
	_ = first.Body.Close()
	_ = second.Body.Close()
}

func TestWebhook404FailsFast(t *testing.T) {
	bucket := newBucketState()
	state := &clientState{buckets: map[uint64]*bucketState{}, identifier: "NoAuth", queueType: NoAuth}
	var calls atomic.Int64
	transport := &scheduledTransport{
		manager: &QueueManager{clusterGlobalRateLimiter: NewClusterGlobalRateLimiter()},
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			return testResponse(req, http.StatusNotFound, nil), nil
		}),
	}
	path := "/webhooks/123/!"

	first, err := transport.RoundTrip(scheduledRequest(context.Background(), state, bucket, path))
	if err != nil {
		t.Fatal(err)
	}
	_ = first.Body.Close()
	second, err := transport.RoundTrip(scheduledRequest(context.Background(), state, bucket, path))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Body.Close()
	if second.StatusCode != http.StatusNotFound || calls.Load() != 1 {
		t.Fatalf("expected cached 404 without another outbound call; status=%d calls=%d", second.StatusCode, calls.Load())
	}
}

func TestUnauthorizedBotFailsFast(t *testing.T) {
	bucket := newBucketState()
	state := &clientState{
		buckets:    map[uint64]*bucketState{},
		identifier: "bot",
		queueType:  Bot,
		botLimit:   50,
		globalHash: 1,
	}
	var calls atomic.Int64
	transport := &scheduledTransport{
		manager: &QueueManager{clusterGlobalRateLimiter: NewClusterGlobalRateLimiter()},
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			return testResponse(req, http.StatusUnauthorized, nil), nil
		}),
	}

	first, err := transport.RoundTrip(scheduledRequest(context.Background(), state, bucket, "/gateway"))
	if err != nil {
		t.Fatal(err)
	}
	_ = first.Body.Close()
	second, err := transport.RoundTrip(scheduledRequest(context.Background(), state, bucket, "/gateway"))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Body.Close()
	if second.StatusCode != http.StatusUnauthorized || calls.Load() != 1 {
		t.Fatalf("expected cached 401 without another outbound call; status=%d calls=%d", second.StatusCode, calls.Load())
	}
}

func TestOutboundTimeoutStartsAfterBucketWait(t *testing.T) {
	oldTimeout := contextTimeout
	contextTimeout = 20 * time.Millisecond
	defer func() { contextTimeout = oldTimeout }()

	bucket := newBucketState()
	bucket.readyAt = time.Now().Add(30 * time.Millisecond)
	state := &clientState{buckets: map[uint64]*bucketState{}, identifier: "NoAuth", queueType: NoAuth}
	transport := &scheduledTransport{
		manager: &QueueManager{clusterGlobalRateLimiter: NewClusterGlobalRateLimiter()},
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			select {
			case <-time.After(2 * time.Millisecond):
				return testResponse(req, http.StatusOK, nil), nil
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		}),
	}

	start := time.Now()
	resp, err := transport.RoundTrip(scheduledRequest(context.Background(), state, bucket, "/gateway"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if elapsed := time.Since(start); elapsed < 25*time.Millisecond {
		t.Fatalf("request skipped bucket wait: %s", elapsed)
	}
}

func TestParseHeaders(t *testing.T) {
	limit, remaining, reset, global, err := parseHeaders(http.Header{
		"X-Ratelimit-Limit":       {"5"},
		"X-Ratelimit-Remaining":   {"0"},
		"X-Ratelimit-Reset-After": {"0.125"},
	}, false)
	if err != nil || limit != 5 || remaining != 0 || reset != 125*time.Millisecond || global {
		t.Fatalf("unexpected parse result: limit=%d remaining=%d reset=%s global=%v err=%v", limit, remaining, reset, global, err)
	}
	if _, _, _, _, err := parseHeaders(http.Header{"Retry-After": {"NaN"}}, true); err == nil {
		t.Fatal("expected invalid reset to be rejected")
	}
}

func TestSweepKeepsActiveBuckets(t *testing.T) {
	state := &clientState{buckets: map[uint64]*bucketState{}}
	idle, active := newBucketState(), newBucketState()
	idle.lastUsed.Store(time.Now().Add(-bucketIdleTimeout - time.Second).UnixNano())
	if err := active.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	active.lastUsed.Store(time.Now().Add(-bucketIdleTimeout - time.Second).UnixNano())
	state.buckets[1], state.buckets[2] = idle, active

	state.sweep(time.Now())
	if state.buckets[1] != nil || state.buckets[2] == nil {
		t.Fatal("sweep did not remove only the idle bucket")
	}
	active.release()
}

func TestClientInitializationIsShared(t *testing.T) {
	manager := NewQueueManager(50, 2)
	defer manager.Shutdown()
	const callers = 20
	states := make(chan *clientState, callers)
	for range callers {
		go func() {
			state, err := manager.getOrCreateClient(context.Background(), "", NoAuth)
			if err != nil {
				t.Errorf("initialization failed: %v", err)
			}
			states <- state
		}()
	}
	first := <-states
	for range callers - 1 {
		if state := <-states; state != first {
			t.Fatal("concurrent initialization created duplicate states")
		}
	}
}

func TestBearerStatesUseLRUEviction(t *testing.T) {
	manager := NewQueueManager(50, 1)
	defer manager.Shutdown()
	first, err := manager.getOrCreateClient(context.Background(), "Bearer first", Bearer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.getOrCreateClient(context.Background(), "Bearer second", Bearer); err != nil {
		t.Fatal(err)
	}
	reloaded, err := manager.getOrCreateClient(context.Background(), "Bearer first", Bearer)
	if err != nil {
		t.Fatal(err)
	}
	if first == reloaded || manager.bearerQueues.Len() != 1 {
		t.Fatal("least-recently-used bearer state was not evicted")
	}
}

func TestProxyPreservesRequestAndResponse(t *testing.T) {
	oldClient, oldTimeout := client, contextTimeout
	seen := make(chan *http.Request, 1)
	client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		seen <- req
		return testResponse(req, http.StatusCreated, http.Header{
			"X-Ratelimit-Limit":     {"10"},
			"X-Ratelimit-Remaining": {"9"},
			"X-Test-Response":       {"preserved"},
		}), nil
	})}
	contextTimeout = time.Second
	manager := NewQueueManager(50, 2)
	defer func() {
		manager.Shutdown()
		client, contextTimeout = oldClient, oldTimeout
	}()

	req := httptest.NewRequest(http.MethodPost, "/api/v10/channels/123/messages?wait=true", strings.NewReader("body"))
	req.Header.Set("Authorization", "Basic abc")
	req.Header.Set("X-Forwarded-For", "192.0.2.1")
	response := httptest.NewRecorder()
	manager.DiscordRequestHandler(response, req)

	outbound := <-seen
	if outbound.URL.Scheme != "https" || outbound.URL.Host != "discord.com" || outbound.URL.Path != req.URL.Path || outbound.URL.RawQuery != "wait=true" {
		t.Fatalf("request target changed unexpectedly: %s", outbound.URL)
	}
	if outbound.Header.Get("Authorization") != "Basic abc" || outbound.Header.Get("X-Forwarded-For") != "192.0.2.1" {
		t.Fatal("request headers were not preserved")
	}
	if response.Code != http.StatusCreated || response.Header().Get("X-Test-Response") != "preserved" || response.Body.String() != "ok" {
		t.Fatalf("response changed unexpectedly: code=%d headers=%v body=%q", response.Code, response.Header(), response.Body.String())
	}
}
