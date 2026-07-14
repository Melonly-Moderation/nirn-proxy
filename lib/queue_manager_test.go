package lib

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGetRequestRoutingInfoBasicAuth(t *testing.T) {
	manager := &QueueManager{}
	req, err := http.NewRequest("POST", "https://discord.com/api/v10/oauth2/token/revoke", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	bucketPath := GetOptimisticBucketPath(req.URL.Path, req.Method)
	hash, queueType := manager.GetRequestRoutingInfo(bucketPath, "Basic ZmFrZVRva2Vu")
	if queueType != NoAuth {
		t.Fatalf("expected queue type %v, got %v", NoAuth, queueType)
	}

	if hash != HashCRC64(bucketPath) {
		t.Fatalf("expected routing hash to match path hash")
	}
}

func TestGlobalLimiterHonorsCancellation(t *testing.T) {
	limiter := NewClusterGlobalRateLimiter()
	if err := limiter.Take(context.Background(), 1, 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := limiter.Take(ctx, 1, 1); err != context.DeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestHandleGlobalValidatesInput(t *testing.T) {
	manager := &QueueManager{clusterGlobalRateLimiter: NewClusterGlobalRateLimiter()}

	post := httptest.NewRequest(http.MethodPost, "/nirn/global", nil)
	postResponse := httptest.NewRecorder()
	manager.HandleGlobal(postResponse, post)
	if postResponse.Code != http.StatusForbidden {
		t.Fatalf("expected forbidden method, got %d", postResponse.Code)
	}

	bad := httptest.NewRequest(http.MethodGet, "/nirn/global", nil)
	badResponse := httptest.NewRecorder()
	manager.HandleGlobal(badResponse, bad)
	if badResponse.Code != http.StatusBadRequest {
		t.Fatalf("expected bad request, got %d", badResponse.Code)
	}

	valid := httptest.NewRequest(http.MethodGet, "/nirn/global", nil)
	valid.Header.Set("bot-hash", "42")
	valid.Header.Set("bot-limit", "50")
	validResponse := httptest.NewRecorder()
	manager.HandleGlobal(validResponse, valid)
	if validResponse.Code != http.StatusOK {
		t.Fatalf("expected success, got %d", validResponse.Code)
	}
}

func TestClusterRoutingAndLoopPrevention(t *testing.T) {
	oldClient, oldTimeout := client, contextTimeout
	client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == "peer:8080" {
			return nil, fmt.Errorf("peer unavailable")
		}
		return testResponse(req, http.StatusOK, http.Header{
			"X-Ratelimit-Limit":     {"10"},
			"X-Ratelimit-Remaining": {"9"},
		}), nil
	})}
	contextTimeout = time.Second
	manager := NewQueueManager(50, 2)
	defer func() {
		manager.Shutdown()
		client, contextTimeout = oldClient, oldTimeout
	}()
	manager.routes.Store(&routeTable{
		members:   []string{"peer"},
		addresses: map[string]string{"peer": "peer:8080"},
		localAddr: "local:8080",
	})

	failedPeer := httptest.NewRequest(http.MethodGet, "/api/v10/gateway", nil)
	failedResponse := httptest.NewRecorder()
	manager.DiscordRequestHandler(failedResponse, failedPeer)
	if failedResponse.Code != http.StatusTooManyRequests || failedResponse.Header().Get("generated-by-proxy") != "true" {
		t.Fatalf("peer failure did not produce retryable 429: code=%d headers=%v", failedResponse.Code, failedResponse.Header())
	}

	routed := httptest.NewRequest(http.MethodGet, "/api/v10/gateway", nil)
	routed.Header.Set("nirn-routed-to", "peer:8080")
	routedResponse := httptest.NewRecorder()
	manager.DiscordRequestHandler(routedResponse, routed)
	if routedResponse.Code != http.StatusOK {
		t.Fatalf("routed request looped instead of being handled locally: %d", routedResponse.Code)
	}
}

func TestGenerated429Shape(t *testing.T) {
	response := httptest.NewRecorder()
	Generate429(response)
	if response.Code != http.StatusTooManyRequests || response.Header().Get("retry-after") != "1" || response.Header().Get("generated-by-proxy") != "true" {
		t.Fatalf("unexpected generated 429: code=%d headers=%v", response.Code, response.Header())
	}
}

func TestGetRequestRoutingInfoBearerAuth(t *testing.T) {
	manager := &QueueManager{}
	req, err := http.NewRequest("GET", "https://discord.com/api/v10/users/@me", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	bucketPath := GetOptimisticBucketPath(req.URL.Path, req.Method)
	hash, queueType := manager.GetRequestRoutingInfo(bucketPath, "Bearer some-token")
	if queueType != Bearer {
		t.Fatalf("expected queue type %v, got %v", Bearer, queueType)
	}

	if hash != HashCRC64("Bearer some-token") {
		t.Fatalf("expected bearer routing hash to use token")
	}
}

func TestGetRequestRoutingInfoBotToken(t *testing.T) {
	manager := &QueueManager{}
	req, err := http.NewRequest("GET", "https://discord.com/api/v10/channels/123/messages", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	bucketPath := GetOptimisticBucketPath(req.URL.Path, req.Method)
	hash, queueType := manager.GetRequestRoutingInfo(bucketPath, "Bot Abc")
	if queueType != Bot {
		t.Fatalf("expected queue type %v, got %v", Bot, queueType)
	}

	if hash != HashCRC64(bucketPath) {
		t.Fatalf("expected bot routing hash to match path hash")
	}
}
