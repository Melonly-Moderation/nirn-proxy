package lib

import (
	"net/http"
	"testing"
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
