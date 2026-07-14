package lib

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const bucketIdleTimeout = 10 * time.Minute

var disable401Lock = EnvGetBool("DISABLE_401_LOCK", false)

type bucketState struct {
	gate fifoGate

	active   atomic.Int64
	lastUsed atomic.Int64

	readyAt time.Time
	fail404 bool
}

func newBucketState() *bucketState {
	b := &bucketState{}
	b.lastUsed.Store(time.Now().UnixNano())
	return b
}

type gateWaiter struct {
	ready    chan struct{}
	granted  bool
	canceled bool
}

type fifoGate struct {
	mu      sync.Mutex
	held    bool
	waiters []*gateWaiter
}

func (g *fifoGate) acquire(ctx context.Context) error {
	g.mu.Lock()
	if !g.held {
		g.held = true
		g.mu.Unlock()
		return nil
	}
	waiter := &gateWaiter{ready: make(chan struct{})}
	g.waiters = append(g.waiters, waiter)
	g.mu.Unlock()

	select {
	case <-waiter.ready:
		return nil
	case <-ctx.Done():
		g.mu.Lock()
		if !waiter.granted {
			waiter.canceled = true
			g.mu.Unlock()
			return ctx.Err()
		}
		g.mu.Unlock()
		g.release()
		return ctx.Err()
	}
}

func (g *fifoGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for len(g.waiters) > 0 {
		waiter := g.waiters[0]
		g.waiters[0] = nil
		g.waiters = g.waiters[1:]
		if waiter.canceled {
			continue
		}
		waiter.granted = true
		close(waiter.ready)
		return
	}
	g.held = false
}

func (b *bucketState) acquire(ctx context.Context) error {
	b.active.Add(1)
	b.lastUsed.Store(time.Now().UnixNano())
	if err := b.gate.acquire(ctx); err != nil {
		b.active.Add(-1)
		return err
	}
	return nil
}

func (b *bucketState) release() {
	b.lastUsed.Store(time.Now().UnixNano())
	b.active.Add(-1)
	b.gate.release()
}

func (b *bucketState) wait(ctx context.Context) error {
	delay := time.Until(b.readyAt)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *bucketState) idle(now time.Time) bool {
	return b.active.Load() == 0 && now.Sub(time.Unix(0, b.lastUsed.Load())) > bucketIdleTimeout
}

type clientState struct {
	mu      sync.RWMutex
	buckets map[uint64]*bucketState

	user       *BotUserResponse
	identifier string
	botLimit   uint
	queueType  QueueType
	globalHash uint64
	invalid    atomic.Bool
}

func newClientState(token string) (*clientState, error) {
	queueType := NoAuth
	var user *BotUserResponse
	var err error

	switch {
	case HasAuthPrefix(token, "Bearer"):
		queueType = Bearer
	case token != "" && !HasAuthPrefix(token, "Basic"):
		queueType = Bot
		user, err = GetBotUser(token)
		if err != nil {
			return nil, err
		}
	}

	limit, err := GetBotGlobalLimit(token, user)
	state := &clientState{
		buckets:    make(map[uint64]*bucketState),
		user:       user,
		identifier: "NoAuth",
		botLimit:   limit,
		queueType:  queueType,
	}
	if err != nil {
		if strings.HasPrefix(err.Error(), "invalid token") {
			state.identifier = "InvalidTokenQueue"
			state.invalid.Store(true)
			return state, nil
		}
		return nil, err
	}

	switch queueType {
	case Bot:
		state.identifier = user.Username + "#" + user.Discrim
		state.globalHash = HashCRC64(user.Id)
	case Bearer:
		state.identifier = "Bearer"
		state.globalHash = HashCRC64(token)
	}

	logger.WithFields(map[string]interface{}{
		"globalLimit": limit,
		"identifier":  state.identifier,
	}).Debug("Created client state")
	return state, nil
}

func (s *clientState) bucket(hash uint64) *bucketState {
	s.mu.RLock()
	b := s.buckets[hash]
	s.mu.RUnlock()
	if b != nil {
		return b
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if b = s.buckets[hash]; b == nil {
		b = newBucketState()
		s.buckets[hash] = b
	}
	return b
}

func (s *clientState) sweep(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for hash, bucket := range s.buckets {
		if bucket.idle(now) {
			delete(s.buckets, hash)
		}
	}
}

func parseHeaders(headers http.Header, preferRetryAfter bool) (int64, int64, time.Duration, bool, error) {
	if headers == nil {
		return 0, 0, 0, false, fmt.Errorf("null headers")
	}

	limit := headers.Get("x-ratelimit-limit")
	remaining := headers.Get("x-ratelimit-remaining")
	resetAfter := headers.Get("x-ratelimit-reset-after")
	retryAfter := headers.Get("retry-after")
	if resetAfter == "" || (preferRetryAfter && retryAfter != "") {
		resetAfter = retryAfter
	}
	isGlobal := headers.Get("x-ratelimit-global") == "true"

	var reset time.Duration
	if resetAfter != "" {
		seconds, err := strconv.ParseFloat(resetAfter, 64)
		if err != nil || seconds < 0 || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
			return 0, 0, 0, false, fmt.Errorf("invalid rate-limit reset %q", resetAfter)
		}
		reset = time.Duration(seconds * float64(time.Second))
	}

	if isGlobal || limit == "" {
		return 0, 0, reset, isGlobal, nil
	}

	limitParsed, err := strconv.ParseInt(limit, 10, 32)
	if err != nil {
		return 0, 0, 0, false, fmt.Errorf("invalid rate-limit limit %q: %w", limit, err)
	}
	remainingParsed, err := strconv.ParseInt(remaining, 10, 32)
	if err != nil {
		return 0, 0, 0, false, fmt.Errorf("invalid rate-limit remaining %q: %w", remaining, err)
	}
	return limitParsed, remainingParsed, reset, false, nil
}

func isInteraction(path string) bool {
	path = strings.SplitN(path, "?", 2)[0]
	for _, part := range strings.Split(path, "/") {
		if len(part) > 128 {
			return true
		}
	}
	return false
}

func syntheticResponse(req *http.Request, status int, headers http.Header, body string) *http.Response {
	return &http.Response{
		Status:        strconv.Itoa(status) + " " + http.StatusText(status),
		StatusCode:    status,
		Header:        headers,
		Body:          io.NopCloser(bytes.NewBufferString(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}

func unauthorizedResponse(req *http.Request) *http.Response {
	return syntheticResponse(req, http.StatusUnauthorized, http.Header{"Content-Type": {"application/json"}}, "{\n\t\"message\": \"401: Unauthorized\",\n\t\"code\": 0\n}")
}

func webhookNotFoundResponse(req *http.Request) *http.Response {
	return syntheticResponse(req, http.StatusNotFound, http.Header{"Content-Type": {"application/json"}}, "{\n  \"message\": \"Unknown Webhook\",\n  \"code\": 10015\n}")
}

func (s *clientState) updateBucket(bucket *bucketState, path string, resp *http.Response) {
	if resp.StatusCode == http.StatusNotFound && strings.HasPrefix(path, "/webhooks/") && !isInteraction(resp.Request.URL.String()) {
		bucket.fail404 = true
	}
	if resp.StatusCode == http.StatusUnauthorized && !isInteraction(resp.Request.URL.String()) && s.queueType != NoAuth && !disable401Lock {
		s.invalid.Store(true)
	}

	scope := resp.Header.Get("x-ratelimit-scope")
	_, remaining, resetAfter, isGlobal, err := parseHeaders(resp.Header, scope != "user")
	if err != nil {
		logger.WithError(err).WithField("path", path).Warn("Ignoring invalid Discord rate-limit headers")
		return
	}

	sharedReaction := resp.StatusCode == http.StatusTooManyRequests && scope == "shared" &&
		(path == "/channels/!/messages/!/reactions/!modify" || path == "/channels/!/messages/!/reactions/!/!")
	if !sharedReaction && (remaining == 0 || resp.StatusCode == http.StatusTooManyRequests) {
		bucket.readyAt = time.Now().Add(resetAfter)
	}

	if resp.StatusCode == http.StatusTooManyRequests && scope != "shared" {
		logger.WithFields(map[string]interface{}{
			"bucket":         path,
			"route":          resp.Request.URL.String(),
			"method":         resp.Request.Method,
			"isGlobal":       isGlobal,
			"discordBucket":  resp.Header.Get("x-ratelimit-bucket"),
			"ratelimitScope": scope,
		}).Warn("Unexpected 429")
	}
}

type releasingBody struct {
	io.ReadCloser
	once    sync.Once
	release func()
}

func (b *releasingBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.release)
	return err
}
