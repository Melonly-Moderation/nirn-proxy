package lib

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

type benchmarkRoundTripFunc func(*http.Request) (*http.Response, error)

func (f benchmarkRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

type discardResponseWriter struct {
	header http.Header
	status int
}

func (w *discardResponseWriter) Header() http.Header            { return w.header }
func (w *discardResponseWriter) WriteHeader(status int)         { w.status = status }
func (w *discardResponseWriter) Write(body []byte) (int, error) { return len(body), nil }

func benchmarkManager(b *testing.B) *QueueManager {
	b.Helper()
	SetLogger(logrus.New())
	oldClient, oldTimeout := client, contextTimeout
	client = &http.Client{Transport: benchmarkRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			Status:        "200 OK",
			StatusCode:    http.StatusOK,
			Header:        http.Header{"X-Ratelimit-Limit": {"10"}, "X-Ratelimit-Remaining": {"9"}},
			Body:          io.NopCloser(strings.NewReader("ok")),
			ContentLength: 2,
			Request:       req,
		}, nil
	})}
	contextTimeout = time.Second
	manager := NewQueueManager(50, 16)
	b.Cleanup(func() {
		manager.Shutdown()
		client, contextTimeout = oldClient, oldTimeout
	})
	return manager
}

func BenchmarkProxyHandlerHotPath(b *testing.B) {
	manager := benchmarkManager(b)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		req := httptest.NewRequest(http.MethodGet, "/api/v10/channels/123456789012345678/messages", nil)
		response := httptest.NewRecorder()
		manager.DiscordRequestHandler(response, req)
		if response.Code != http.StatusOK {
			b.Fatalf("unexpected status %d", response.Code)
		}
	}
}

func BenchmarkProxyBurst(b *testing.B) {
	manager := benchmarkManager(b)
	const sampleLimit = 10_000
	samples := make([]int64, sampleLimit)
	var sampleCount atomic.Uint64

	b.ReportAllocs()
	b.SetParallelism(4)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		requestNumber := 0
		for pb.Next() {
			path := fmt.Sprintf("/api/v10/channels/%018d/messages", 123456789012345678+requestNumber%64)
			start := time.Now()
			req := httptest.NewRequest(http.MethodGet, path, nil)
			response := httptest.NewRecorder()
			manager.DiscordRequestHandler(response, req)
			elapsed := time.Since(start).Nanoseconds()
			if index := sampleCount.Add(1) - 1; index < sampleLimit {
				samples[index] = elapsed
			}
			if response.Code != http.StatusOK {
				b.Errorf("unexpected status %d", response.Code)
			}
			requestNumber++
		}
	})
	b.StopTimer()
	count := min(int(sampleCount.Load()), sampleLimit)
	if count > 0 {
		sort.Slice(samples[:count], func(i, j int) bool { return samples[i] < samples[j] })
		b.ReportMetric(float64(samples[(count-1)*99/100]), "p99-ns")
	}
}

func BenchmarkProxy1000RPS(b *testing.B) {
	manager := benchmarkManager(b)
	samples := make([]int64, b.N)
	requests := make([]*http.Request, b.N)
	responses := make([]*discardResponseWriter, b.N)
	for i := range b.N {
		path := fmt.Sprintf("/api/v10/channels/%018d/messages", 123456789012345678+i%64)
		requests[i] = httptest.NewRequest(http.MethodGet, path, nil)
		responses[i] = &discardResponseWriter{header: make(http.Header)}
	}
	start := time.Now()

	b.ResetTimer()
	for i := range b.N {
		if delay := time.Until(start.Add(time.Duration(i) * time.Millisecond)); delay > 0 {
			time.Sleep(delay)
		}
		requestStart := time.Now()
		manager.DiscordRequestHandler(responses[i], requests[i])
		samples[i] = time.Since(requestStart).Nanoseconds()
		if responses[i].status != http.StatusOK {
			b.Errorf("unexpected status %d", responses[i].status)
		}
	}
	b.StopTimer()
	if len(samples) > 0 {
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		b.ReportMetric(float64(samples[(len(samples)-1)*99/100]), "service-p99-ns")
	}
}
