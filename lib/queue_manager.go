package lib

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/hashicorp/memberlist"
	"github.com/sirupsen/logrus"
)

type QueueType int64

const (
	Bot QueueType = iota
	NoAuth
	Bearer
)

var pathsToRouteLocally = map[uint64]struct{}{
	HashCRC64("/users/@me/channels"): {},
	HashCRC64("/users/@me"):          {},
}

type managerContextKey string

const (
	requestMetadataKey managerContextKey = "request-metadata"
	peerAddressKey     managerContextKey = "peer-address"
)

type requestMetadata struct {
	state       *clientState
	bucket      *bucketState
	bucketPath  string
	metricsPath string
}

type clientEntry struct {
	ready chan struct{}
	state *clientState
	err   error
}

type routeTable struct {
	members   []string
	addresses map[string]string
	localAddr string
}

type scheduledTransport struct {
	base    http.RoundTripper
	manager *QueueManager
}

type proxyBufferPool struct{ pool sync.Pool }

func (p *proxyBufferPool) Get() []byte {
	if buffer := p.pool.Get(); buffer != nil {
		return buffer.([]byte)
	}
	return make([]byte, 32*1024)
}

func (p *proxyBufferPool) Put(buffer []byte) { p.pool.Put(buffer) }

type timeoutTransport struct {
	base    http.RoundTripper
	timeout time.Duration
}

func (t *timeoutTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(req.Context(), t.timeout)
	resp, err := t.base.RoundTrip(req.WithContext(ctx))
	if err != nil {
		cancel()
		return nil, err
	}
	resp.Body = &releasingBody{ReadCloser: resp.Body, release: cancel}
	return resp, nil
}

func preserveForwardingHeaders(proxyReq *httputil.ProxyRequest) {
	for _, name := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
		if values, ok := proxyReq.In.Header[name]; ok {
			proxyReq.Out.Header[name] = append([]string(nil), values...)
		}
	}
	if proxyReq.In.Header.Get("User-Agent") == "" {
		proxyReq.Out.Header.Set("User-Agent", "Go-http-client/1.1")
	}
}

func (t *scheduledTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	meta, ok := req.Context().Value(requestMetadataKey).(*requestMetadata)
	if !ok {
		return nil, fmt.Errorf("missing request scheduler metadata")
	}

	if err := meta.bucket.acquire(req.Context()); err != nil {
		return nil, err
	}
	release := meta.bucket.release

	if meta.state.invalid.Load() {
		resp := unauthorizedResponse(req)
		resp.Body = &releasingBody{ReadCloser: resp.Body, release: release}
		return resp, nil
	}
	if meta.bucket.fail404 {
		resp := webhookNotFoundResponse(req)
		resp.Body = &releasingBody{ReadCloser: resp.Body, release: release}
		return resp, nil
	}
	if err := meta.bucket.wait(req.Context()); err != nil {
		release()
		return nil, err
	}
	if err := t.manager.acquireGlobal(req.Context(), meta.state); err != nil {
		release()
		return nil, err
	}

	ctx, cancel := context.WithTimeout(req.Context(), contextTimeout)
	outbound := req.WithContext(ctx)
	start := time.Now()
	resp, err := t.base.RoundTrip(outbound)
	if err != nil {
		cancel()
		release()
		return nil, err
	}

	meta.state.updateBucket(meta.bucket, meta.bucketPath, resp)
	status := resp.Status
	if resp.StatusCode == http.StatusTooManyRequests && resp.Header.Get("x-ratelimit-scope") == "shared" {
		status = "429 Shared"
	}
	RequestHistogram.WithLabelValues(req.Method, status, meta.metricsPath, meta.state.identifier).Observe(time.Since(start).Seconds())
	if logger.IsLevelEnabled(logrus.DebugLevel) {
		logger.WithFields(logrus.Fields{
			"method":        req.Method,
			"path":          req.URL.String(),
			"status":        resp.Status,
			"discordBucket": resp.Header.Get("x-ratelimit-bucket"),
		}).Debug("Discord request")
	}

	resp.Body = &releasingBody{
		ReadCloser: resp.Body,
		release: func() {
			cancel()
			release()
		},
	}
	return resp, nil
}

type QueueManager struct {
	botMu  sync.Mutex
	queues map[[sha256.Size]byte]*clientEntry

	bearerMu     sync.Mutex
	bearerQueues *lru.Cache

	clusterMu sync.RWMutex
	cluster   *memberlist.Memberlist
	localAddr string
	routes    atomic.Pointer[routeTable]

	clusterGlobalRateLimiter *ClusterGlobalRateLimiter
	discordProxy             *httputil.ReverseProxy
	peerProxy                *httputil.ReverseProxy
	discordURL               *url.URL

	stop     chan struct{}
	stopOnce sync.Once
}

func NewQueueManager(bufferSize int, maxBearerLruSize int) *QueueManager {
	// ponytail: BUFFER_SIZE remains accepted for config compatibility; the scheduler has no channel buffer to size.
	_ = bufferSize
	if maxBearerLruSize <= 0 {
		panic("MAX_BEARER_COUNT must be greater than zero")
	}
	bearerMap, err := lru.New(maxBearerLruSize)
	if err != nil {
		panic(err)
	}

	base := http.DefaultTransport
	if client != nil && client.Transport != nil {
		base = client.Transport
	}
	discordURL, _ := url.Parse("https://discord.com")
	m := &QueueManager{
		queues:                   make(map[[sha256.Size]byte]*clientEntry),
		bearerQueues:             bearerMap,
		clusterGlobalRateLimiter: NewClusterGlobalRateLimiter(),
		discordURL:               discordURL,
		stop:                     make(chan struct{}),
	}
	buffers := &proxyBufferPool{}
	m.discordProxy = &httputil.ReverseProxy{
		Rewrite: func(proxyReq *httputil.ProxyRequest) {
			proxyReq.SetURL(m.discordURL)
			preserveForwardingHeaders(proxyReq)
		},
		Transport:  &scheduledTransport{base: base, manager: m},
		BufferPool: buffers,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			status := http.StatusInternalServerError
			if errors.Is(err, context.DeadlineExceeded) {
				status = http.StatusRequestTimeout
			}
			w.WriteHeader(status)
			_, _ = w.Write([]byte(err.Error()))
			if !errors.Is(err, context.Canceled) {
				logger.WithError(err).WithField("function", "discordProxy").Error("Discord request failed")
			}
		},
	}
	m.peerProxy = &httputil.ReverseProxy{
		Rewrite: func(proxyReq *httputil.ProxyRequest) {
			addr, _ := proxyReq.In.Context().Value(peerAddressKey).(string)
			proxyReq.SetURL(&url.URL{Scheme: "http", Host: addr})
			preserveForwardingHeaders(proxyReq)
			proxyReq.Out.Header.Set("nirn-routed-to", addr)
		},
		Transport:  &timeoutTransport{base: base, timeout: 90 * time.Second},
		BufferPool: buffers,
		ModifyResponse: func(_ *http.Response) error {
			RequestsRoutedSent.Inc()
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			RequestsRoutedError.Inc()
			logger.WithError(err).WithField("function", "peerProxy").Warn("Cluster request failed")
			Generate429(w)
		},
	}
	go m.sweepLoop()
	return m
}

func (m *QueueManager) sweepLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			m.botMu.Lock()
			states := make([]*clientState, 0, len(m.queues))
			for _, entry := range m.queues {
				select {
				case <-entry.ready:
					if entry.state != nil {
						states = append(states, entry.state)
					}
				default:
				}
			}
			m.botMu.Unlock()
			for _, state := range states {
				state.sweep(now)
			}
		case <-m.stop:
			return
		}
	}
}

func (m *QueueManager) Shutdown() {
	m.stopOnce.Do(func() { close(m.stop) })
	m.clusterMu.RLock()
	cluster := m.cluster
	m.clusterMu.RUnlock()
	if cluster != nil {
		_ = cluster.Leave(30 * time.Second)
	}
}

func (m *QueueManager) reindexMembers() {
	m.clusterMu.RLock()
	cluster := m.cluster
	localAddr := m.localAddr
	m.clusterMu.RUnlock()
	if cluster == nil {
		return
	}

	members := cluster.Members()
	table := &routeTable{
		members:   make([]string, 0, len(members)),
		addresses: make(map[string]string, len(members)),
		localAddr: localAddr,
	}
	for _, node := range members {
		table.members = append(table.members, node.Name)
		table.addresses[node.Name] = node.Addr.String() + ":" + string(node.Meta)
	}
	sort.Strings(table.members)
	m.routes.Store(table)
}

func (m *QueueManager) onNodeJoin(_ *memberlist.Node)  { go m.reindexMembers() }
func (m *QueueManager) onNodeLeave(_ *memberlist.Node) { go m.reindexMembers() }

func (m *QueueManager) GetEventDelegate() *NirnEvents {
	return &NirnEvents{OnJoin: m.onNodeJoin, OnLeave: m.onNodeLeave}
}

func (m *QueueManager) SetCluster(cluster *memberlist.Memberlist, proxyPort string) {
	m.clusterMu.Lock()
	m.cluster = cluster
	m.localAddr = cluster.LocalNode().Addr.String() + ":" + proxyPort
	m.clusterMu.Unlock()
	m.reindexMembers()
}

func (m *QueueManager) calculateRoute(pathHash uint64) string {
	if pathHash == 0 {
		return ""
	}
	if _, local := pathsToRouteLocally[pathHash]; local {
		return ""
	}
	table := m.routes.Load()
	if table == nil || len(table.members) == 0 {
		return ""
	}
	name := table.members[pathHash%uint64(len(table.members))]
	addr := table.addresses[name]
	if addr == table.localAddr {
		return ""
	}
	return addr
}

func Generate429(w http.ResponseWriter) {
	w.Header().Set("generated-by-proxy", "true")
	w.Header().Set("x-ratelimit-scope", "user")
	w.Header().Set("x-ratelimit-limit", "1")
	w.Header().Set("x-ratelimit-remaining", "0")
	w.Header().Set("x-ratelimit-reset", strconv.FormatInt(time.Now().Add(time.Second).Unix(), 10))
	w.Header().Set("x-ratelimit-after", "1")
	w.Header().Set("retry-after", "1")
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = w.Write([]byte("{\n\t\"global\": false,\n\t\"message\": \"You are being rate limited.\",\n\t\"retry_after\": 1\n}"))
}

func (m *QueueManager) getOrCreateClient(ctx context.Context, token string, queueType QueueType) (*clientState, error) {
	key := sha256.Sum256([]byte(token))
	entry := (*clientEntry)(nil)
	created := false

	if queueType == Bearer {
		m.bearerMu.Lock()
		if cached, ok := m.bearerQueues.Get(key); ok {
			entry = cached.(*clientEntry)
		} else {
			entry = &clientEntry{ready: make(chan struct{})}
			m.bearerQueues.Add(key, entry)
			created = true
		}
		m.bearerMu.Unlock()
	} else {
		m.botMu.Lock()
		entry = m.queues[key]
		if entry == nil {
			entry = &clientEntry{ready: make(chan struct{})}
			m.queues[key] = entry
			created = true
		}
		m.botMu.Unlock()
	}

	if created {
		entry.state, entry.err = newClientState(token)
		close(entry.ready)
		if entry.err != nil {
			if queueType == Bearer {
				m.bearerMu.Lock()
				if current, ok := m.bearerQueues.Get(key); ok && current == entry {
					m.bearerQueues.Remove(key)
				}
				m.bearerMu.Unlock()
			} else {
				m.botMu.Lock()
				if m.queues[key] == entry {
					delete(m.queues, key)
				}
				m.botMu.Unlock()
			}
		}
	}

	select {
	case <-entry.ready:
		return entry.state, entry.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *QueueManager) acquireGlobal(ctx context.Context, state *clientState) error {
	if state.queueType == NoAuth {
		return nil
	}
	if state.queueType == Bearer {
		return m.clusterGlobalRateLimiter.Take(ctx, state.globalHash, state.botLimit)
	}
	if addr := m.calculateRoute(state.globalHash); addr != "" {
		return m.clusterGlobalRateLimiter.FireGlobalRequest(ctx, addr, state.globalHash, state.botLimit)
	}
	return m.clusterGlobalRateLimiter.Take(ctx, state.globalHash, state.botLimit)
}

func (m *QueueManager) DiscordRequestHandler(w http.ResponseWriter, req *http.Request) {
	bucketPath := GetOptimisticBucketPath(req.URL.Path, req.Method)
	metricsPath := MetricsPathFromBucket(bucketPath)
	openConnections := ConnectionsOpen.WithLabelValues(req.Method, metricsPath)
	openConnections.Inc()
	defer openConnections.Dec()

	token := req.Header.Get("Authorization")
	routingHash, queueType := m.GetRequestRoutingInfo(bucketPath, token)
	routeTo := m.calculateRoute(routingHash)
	routedTo := req.Header.Get("nirn-routed-to")
	req.Header.Del("nirn-routed-to")
	if routedTo != "" {
		RequestsRoutedRecv.Inc()
	}
	if routeTo != "" && routedTo == "" {
		ctx := context.WithValue(req.Context(), peerAddressKey, routeTo)
		m.peerProxy.ServeHTTP(w, req.WithContext(ctx))
		return
	}

	state, err := m.getOrCreateClient(req.Context(), token, queueType)
	if err != nil {
		if strings.HasPrefix(err.Error(), "429") {
			Generate429(w)
		} else if !errors.Is(err, context.Canceled) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			logger.WithError(err).WithField("function", "getOrCreateClient").Error("Failed to initialize client")
		}
		return
	}

	bucketHash := routingHash
	if queueType == Bearer {
		bucketHash = HashCRC64(bucketPath)
	}
	meta := &requestMetadata{
		state:       state,
		bucket:      state.bucket(bucketHash),
		bucketPath:  bucketPath,
		metricsPath: metricsPath,
	}
	ctx := context.WithValue(req.Context(), requestMetadataKey, meta)
	m.discordProxy.ServeHTTP(w, req.WithContext(ctx))
}

func (m *QueueManager) GetRequestRoutingInfo(bucketPath string, token string) (routingHash uint64, queueType QueueType) {
	queueType = NoAuth
	routingHash = HashCRC64(bucketPath)
	switch {
	case HasAuthPrefix(token, "Bearer"):
		queueType = Bearer
		routingHash = HashCRC64(token)
	case token != "" && !HasAuthPrefix(token, "Basic"):
		queueType = Bot
	}
	return
}

func (m *QueueManager) HandleGlobal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	botHash, err := strconv.ParseUint(r.Header.Get("bot-hash"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	botLimit, err := strconv.ParseUint(r.Header.Get("bot-limit"), 10, 32)
	if err != nil || botLimit == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := m.clusterGlobalRateLimiter.Take(r.Context(), botHash, uint(botLimit)); err != nil {
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (m *QueueManager) CreateMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/nirn/global", m.HandleGlobal)
	mux.HandleFunc("/nirn/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/", m.DiscordRequestHandler)
	return mux
}
