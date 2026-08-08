package proxy

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	retryDrainLimit  = 64 << 10
	retryMemoryLimit = 1 << 20
)

var (
	errRetryCaptureBudget = errors.New("retry capture capacity exhausted")
	errRetryPreparation   = errors.New("retry body preparation failed")
)

type proxyBufferPool struct{ pool sync.Pool }

func (p *proxyBufferPool) Get() []byte {
	if buffer := p.pool.Get(); buffer != nil {
		return buffer.([]byte)
	}
	return make([]byte, 32*1024)
}

func (p *proxyBufferPool) Put(buffer []byte) { p.pool.Put(buffer) }

type cleanupBody struct {
	io.ReadCloser
	once    sync.Once
	cleanup func()
}

func (b *cleanupBody) Close() error {
	b.once.Do(b.cleanup)
	return b.ReadCloser.Close()
}

type timeoutTransport struct {
	base    http.RoundTripper
	timeout time.Duration
}

type clusterPeerRoundTripper struct{ proxy *Proxy }

func (t clusterPeerRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	t.proxy.clusterMu.RLock()
	transport := t.proxy.peerTransport
	t.proxy.clusterMu.RUnlock()
	if transport == nil {
		return nil, fmt.Errorf("cluster peer transport is not configured")
	}
	return transport.RoundTrip(request)
}

func (t *timeoutTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(request.Context(), t.timeout)
	response, err := t.base.RoundTrip(request.WithContext(ctx))
	if err != nil {
		cancel()
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, err
	}
	if response == nil {
		cancel()
		return nil, fmt.Errorf("upstream transport returned no response")
	}
	if response.Body == nil {
		response.Body = http.NoBody
	}
	response.Body = &cleanupBody{ReadCloser: response.Body, cleanup: cancel}
	return response, nil
}

type scheduledTransport struct {
	base  http.RoundTripper
	proxy *Proxy
}

func (t *scheduledTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	metadata, ok := request.Context().Value(requestMetadataContextKey).(*requestMetadata)
	if !ok {
		return nil, fmt.Errorf("missing request scheduler metadata")
	}

	queueContext, cancelQueue := context.WithTimeout(request.Context(), t.proxy.config.QueueTimeout)
	queueCleanupPending := true
	defer func() {
		if queueCleanupPending {
			cancelQueue()
		}
	}()
	bucket, err := metadata.state.acquireBucket(queueContext, metadata.routeHash)
	if err != nil {
		return nil, err
	}
	bucketHeld := true
	defer func() {
		if bucketHeld {
			bucket.release()
		}
	}()

	releaseBucket := func() {
		if bucketHeld {
			bucket.release()
			bucketHeld = false
		}
	}

	validationHeld := false
	if !metadata.interaction && metadata.state.validity.Load() == clientUnknown {
		if err := metadata.state.validation.acquire(queueContext); err != nil {
			return nil, err
		}
		validationHeld = true
		if metadata.state.validity.Load() != clientUnknown {
			metadata.state.validation.release()
			validationHeld = false
		}
	}
	defer func() {
		if validationHeld {
			metadata.state.validation.release()
		}
	}()

	if metadata.state.validity.Load() == clientInvalid {
		return unauthorizedResponse(request), nil
	}

	replay, err := newBodyReplay(request, t.proxy.config.MaxRetryBodyBytes, t.proxy.retryCapture)
	if err != nil {
		return nil, err
	}
	defer replay.close()
	for attempt := 0; ; attempt++ {
		if err := bucket.wait(queueContext); err != nil {
			return nil, err
		}
		if !metadata.interaction {
			if err := metadata.state.global.wait(queueContext); err != nil {
				return nil, err
			}
		}

		body, err := replay.body(attempt)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errRetryPreparation, err)
		}
		attemptContext, cancelAttempt := context.WithTimeout(queueContext, t.proxy.config.UpstreamTimeout)
		outbound := request.Clone(attemptContext)
		outbound.Body = body
		if !t.proxy.invalidRequests.reserve(time.Now()) {
			if body != nil {
				_ = body.Close()
			}
			cancelAttempt()
			return nil, errInvalidRequestBudget
		}
		var started time.Time
		if t.proxy.config.EnableMetrics {
			started = time.Now()
		}
		response, err := t.base.RoundTrip(outbound)
		if err != nil {
			t.proxy.invalidRequests.complete(time.Now(), false)
			cancelAttempt()
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			return nil, err
		}
		if response == nil {
			t.proxy.invalidRequests.complete(time.Now(), false)
			cancelAttempt()
			return nil, fmt.Errorf("upstream transport returned no response")
		}
		if response.Body == nil {
			response.Body = http.NoBody
		}
		t.proxy.invalidRequests.complete(time.Now(), invalidDiscordResponse(response))

		metadata.state.observeResponse(
			metadata.routeHash,
			metadata.majorKey,
			metadata.bucketPath,
			metadata.interaction,
			bucket,
			response,
			t.proxy.config.Disable401Lock,
		)
		if validationHeld {
			if response.StatusCode == http.StatusUnauthorized && !t.proxy.config.Disable401Lock {
				metadata.state.validity.Store(clientInvalid)
			} else {
				metadata.state.validity.Store(clientValid)
			}
			metadata.state.validation.release()
			validationHeld = false
		}

		if t.proxy.config.EnableMetrics {
			status := response.Status
			if response.StatusCode == http.StatusTooManyRequests && response.Header.Get("X-RateLimit-Scope") == "shared" {
				status = "429 Shared"
			}
			RequestHistogram.WithLabelValues(metadata.metricsMethod, status, metadata.metricsPath, metadata.state.identity.label).Observe(time.Since(started).Seconds())
		}
		if logger.IsLevelEnabled(logrus.DebugLevel) {
			logger.WithFields(logrus.Fields{
				"method":        request.Method,
				"path":          metadata.metricsPath,
				"status":        response.Status,
				"discordBucket": response.Header.Get("X-RateLimit-Bucket"),
			}).Debug("Discord request")
		}

		if response.StatusCode != http.StatusTooManyRequests {
			releaseBucket()
			response.Body = &cleanupBody{ReadCloser: response.Body, cleanup: func() {
				cancelAttempt()
				cancelQueue()
			}}
			queueCleanupPending = false
			return response, nil
		}
		retryable, prepareErr := replay.prepareRetry()
		if prepareErr != nil {
			cancelAttempt()
			_ = response.Body.Close()
			return nil, fmt.Errorf("%w: %v", errRetryPreparation, prepareErr)
		}
		if !retryable {
			releaseBucket()
			response.Body = &cleanupBody{ReadCloser: response.Body, cleanup: func() {
				cancelAttempt()
				cancelQueue()
			}}
			queueCleanupPending = false
			return response, nil
		}

		logger.WithFields(logrus.Fields{
			"bucket":        metadata.bucketPath,
			"route":         metadata.metricsPath,
			"scope":         response.Header.Get("X-RateLimit-Scope"),
			"discordBucket": response.Header.Get("X-RateLimit-Bucket"),
		}).Debug("Discord rate limit absorbed; retrying")
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, retryDrainLimit))
		cancelAttempt()
		_ = response.Body.Close()

		releaseBucket()
		bucket, err = metadata.state.acquireBucket(queueContext, metadata.routeHash)
		if err != nil {
			return nil, err
		}
		bucketHeld = true
	}
}

type captureBody struct {
	io.ReadCloser
	mu            sync.Mutex
	buffer        bytes.Buffer
	file          *os.File
	fileName      string
	limit         int64
	contentLength int64
	read          int64
	complete      bool
	overflow      bool
	sealed        bool
	budget        *resourceBudget
	reserved      int64
}

func (b *captureBody) Read(target []byte) (int, error) {
	read, err := b.ReadCloser.Read(target)
	b.mu.Lock()
	defer b.mu.Unlock()
	if read > 0 {
		b.read += int64(read)
		b.captureLocked(target[:read])
	}
	if err == io.EOF || b.contentLength >= 0 && b.read == b.contentLength {
		b.complete = true
	}
	return read, err
}

func (b *captureBody) captureLocked(data []byte) {
	if b.overflow || b.sealed {
		return
	}
	if b.read > b.limit {
		b.discardLocked()
		return
	}
	if additional := b.read - b.reserved; additional > 0 {
		if !b.budget.reserve(additional) {
			b.discardLocked()
			return
		}
		b.reserved += additional
	}
	if b.file == nil && int64(b.buffer.Len()+len(data)) <= min(b.limit, retryMemoryLimit) {
		_, _ = b.buffer.Write(data)
		return
	}
	if b.file == nil {
		file, err := os.CreateTemp("", "nirn-retry-*")
		if err != nil {
			b.discardLocked()
			return
		}
		b.file, b.fileName = file, file.Name()
		if _, err := b.file.Write(b.buffer.Bytes()); err != nil {
			b.discardLocked()
			return
		}
		b.buffer = bytes.Buffer{}
	}
	if _, err := b.file.Write(data); err != nil {
		b.discardLocked()
	}
}

func (b *captureBody) discardLocked() {
	b.overflow = true
	b.buffer = bytes.Buffer{}
	if b.file != nil {
		_ = b.file.Close()
		b.file = nil
	}
	if b.fileName != "" {
		_ = os.Remove(b.fileName)
		b.fileName = ""
	}
	b.releaseReservationLocked()
}

func (b *captureBody) releaseReservationLocked() {
	if b.reserved == 0 {
		return
	}
	b.budget.release(b.reserved)
	b.reserved = 0
}

func (b *captureBody) replay() ([]byte, string, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.complete || b.overflow {
		return nil, "", false, nil
	}
	b.sealed = true
	if b.file == nil {
		data := b.buffer.Bytes()
		b.buffer = bytes.Buffer{}
		return data, "", true, nil
	}
	if err := b.file.Close(); err != nil {
		b.file = nil
		return nil, "", false, err
	}
	b.file = nil
	return nil, b.fileName, true, nil
}

func (b *captureBody) cleanup() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.discardLocked()
}

type bodyReplay struct {
	first    io.ReadCloser
	getBody  func() (io.ReadCloser, error)
	capture  *captureBody
	data     []byte
	fileName string
	prepared bool
}

func newBodyReplay(request *http.Request, limit int64, budget *resourceBudget) (*bodyReplay, error) {
	if request.Body == nil || request.Body == http.NoBody {
		return &bodyReplay{}, nil
	}
	if request.GetBody != nil {
		return &bodyReplay{first: request.Body, getBody: request.GetBody}, nil
	}
	contentLength := request.ContentLength
	if contentLength == 0 {
		// net/http treats zero with a non-nil request body as an unknown length.
		contentLength = -1
	}
	capture := &captureBody{ReadCloser: request.Body, limit: limit, contentLength: contentLength, budget: budget}
	if contentLength > limit {
		capture.overflow = true
	} else if budget.limit == 0 {
		capture.overflow = true
	} else if contentLength > 0 {
		if !budget.reserve(contentLength) {
			return nil, errRetryCaptureBudget
		}
		capture.reserved = contentLength
	}
	return &bodyReplay{first: capture, capture: capture}, nil
}

func (r *bodyReplay) body(attempt int) (io.ReadCloser, error) {
	if attempt == 0 {
		return r.first, nil
	}
	if r.first == nil && r.getBody == nil && r.capture == nil {
		return nil, nil
	}
	if r.getBody != nil {
		return r.getBody()
	}
	if !r.prepared {
		ok, err := r.prepareRetry()
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("request body cannot be replayed")
		}
	}
	if r.fileName != "" {
		return os.Open(r.fileName)
	}
	if r.capture == nil {
		return nil, nil
	}
	return io.NopCloser(bytes.NewReader(r.data)), nil
}

func (r *bodyReplay) prepareRetry() (bool, error) {
	if r.getBody != nil || r.first == nil {
		r.prepared = true
		return true, nil
	}
	if r.prepared {
		return true, nil
	}
	data, fileName, ok, err := r.capture.replay()
	if err != nil {
		return false, fmt.Errorf("finish retry body: %w", err)
	}
	if !ok {
		return false, nil
	}
	r.data, r.fileName, r.prepared = data, fileName, true
	return true, nil
}

func (r *bodyReplay) close() {
	if r.capture == nil {
		return
	}
	r.capture.cleanup()
	r.fileName = ""
}

func syntheticResponse(request *http.Request, status int, body string) *http.Response {
	return &http.Response{
		Status:        strconv.Itoa(status) + " " + http.StatusText(status),
		StatusCode:    status,
		Header:        http.Header{"Content-Type": {"application/json"}},
		Body:          io.NopCloser(bytes.NewBufferString(body)),
		ContentLength: int64(len(body)),
		Request:       request,
	}
}

func unauthorizedResponse(request *http.Request) *http.Response {
	return syntheticResponse(request, http.StatusUnauthorized, "{\n\t\"message\": \"401: Unauthorized\",\n\t\"code\": 0\n}")
}

func newHTTPTransport(outboundIP string, disableHTTP2 bool) (*http.Transport, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 2048
	transport.MaxIdleConnsPerHost = 1024
	transport.MaxConnsPerHost = 1024
	transport.IdleConnTimeout = 90 * time.Second
	transport.TLSHandshakeTimeout = 10 * time.Second
	transport.ExpectContinueTimeout = time.Second
	transport.ForceAttemptHTTP2 = !disableHTTP2

	if outboundIP != "" {
		address, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(outboundIP, "0"))
		if err != nil {
			return nil, fmt.Errorf("resolve OUTBOUND_IP: %w", err)
		}
		dialer := &net.Dialer{LocalAddr: address, Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
		transport.DialContext = dialer.DialContext
	}
	if disableHTTP2 {
		transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	}
	return transport, nil
}

func newPeerHTTPTransport(config *tls.Config) (*http.Transport, error) {
	if config == nil {
		return nil, fmt.Errorf("cluster peer TLS configuration is required")
	}
	if config.InsecureSkipVerify {
		return nil, fmt.Errorf("cluster peer TLS cannot disable certificate verification")
	}
	if config.RootCAs == nil {
		return nil, fmt.Errorf("cluster peer TLS requires an explicit CA pool")
	}
	if len(config.Certificates) == 0 && config.GetClientCertificate == nil {
		return nil, fmt.Errorf("cluster peer TLS requires a client certificate")
	}
	if config.MaxVersion != 0 && config.MaxVersion < tls.VersionTLS13 {
		return nil, fmt.Errorf("cluster peer TLS requires TLS 1.3")
	}
	tlsConfig := config.Clone()
	tlsConfig.MinVersion = max(tlsConfig.MinVersion, tls.VersionTLS13)
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	return &http.Transport{
		Proxy:                 nil,
		DisableCompression:    true,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   256,
		MaxConnsPerHost:       256,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		TLSClientConfig:       tlsConfig,
	}, nil
}

func (p *Proxy) initReverseProxies() {
	buffers := &proxyBufferPool{}
	p.discordProxy = &httputil.ReverseProxy{
		Rewrite: func(proxyRequest *httputil.ProxyRequest) {
			proxyRequest.SetURL(p.discordURL)
			preserveForwardingHeaders(proxyRequest)
		},
		Transport:  &scheduledTransport{base: p.transport, proxy: p},
		BufferPool: buffers,
		ErrorHandler: func(writer http.ResponseWriter, _ *http.Request, err error) {
			switch {
			case errors.Is(err, context.Canceled):
				select {
				case <-p.ctx.Done():
					writeUnavailable(writer, "proxy is shutting down")
				default:
					return
				}
			case errors.Is(err, context.DeadlineExceeded):
				http.Error(writer, err.Error(), http.StatusRequestTimeout)
			case errors.Is(err, errQueueFull), errors.Is(err, errTooManyClients), errors.Is(err, errBucketStateLimit), errors.Is(err, errInvalidRequestBudget), errors.Is(err, errRetryCaptureBudget), errors.Is(err, errRetryPreparation):
				writeUnavailable(writer, err.Error())
			default:
				http.Error(writer, "Discord upstream unavailable", http.StatusBadGateway)
				logger.WithError(err).WithField("function", "discordProxy").Error("Discord request failed")
			}
		},
	}
	p.peerProxy = &httputil.ReverseProxy{
		Rewrite: func(proxyRequest *httputil.ProxyRequest) {
			target, _ := proxyRequest.In.Context().Value(peerTargetContextKey).(peerTarget)
			proxyRequest.SetURL(&url.URL{Scheme: "https", Host: target.address})
			preserveForwardingHeaders(proxyRequest)
			proxyRequest.Out.Header.Set("X-Nirn-Hop", strconv.Itoa(target.hop))
		},
		Transport:  &timeoutTransport{base: clusterPeerRoundTripper{proxy: p}, timeout: p.config.QueueTimeout + p.config.UpstreamTimeout + 5*time.Second},
		BufferPool: buffers,
		ModifyResponse: func(_ *http.Response) error {
			if p.config.EnableMetrics {
				RequestsRoutedSent.Inc()
			}
			return nil
		},
		ErrorHandler: func(writer http.ResponseWriter, _ *http.Request, err error) {
			if p.config.EnableMetrics {
				RequestsRoutedError.Inc()
			}
			logger.WithError(err).WithField("function", "peerProxy").Warn("Cluster request failed")
			writeUnavailable(writer, "cluster peer unavailable")
		},
	}
}

func preserveForwardingHeaders(proxyRequest *httputil.ProxyRequest) {
	for _, name := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
		if values, ok := proxyRequest.In.Header[name]; ok {
			proxyRequest.Out.Header[name] = append([]string(nil), values...)
		}
	}
	if proxyRequest.In.Header.Get("User-Agent") == "" {
		proxyRequest.Out.Header.Set("User-Agent", "DiscordBot (https://github.com/Melonly-Moderation/nirn-proxy, dev)")
	}
}
