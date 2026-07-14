package lib

import (
	"context"
	"errors"
	"github.com/Clever/leakybucket"
	"github.com/Clever/leakybucket/memory"
	"github.com/sirupsen/logrus"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type ClusterGlobalRateLimiter struct {
	sync.RWMutex
	globalBucketsMap map[uint64]*leakybucket.Bucket
	memStorage       *memory.Storage
}

func NewClusterGlobalRateLimiter() *ClusterGlobalRateLimiter {
	memStorage := memory.New()
	return &ClusterGlobalRateLimiter{
		memStorage:       memStorage,
		globalBucketsMap: make(map[uint64]*leakybucket.Bucket),
	}
}

func (c *ClusterGlobalRateLimiter) Take(ctx context.Context, botHash uint64, botLimit uint) error {
	bucket := c.getOrCreate(botHash, botLimit)
	for {
		_, err := (*bucket).Add(1)
		if err == nil {
			return nil
		}
		reset := (*bucket).Reset()
		delay := time.Until(reset)
		logger.WithFields(logrus.Fields{
			"waitTime": delay,
		}).Trace("Failed to grab global token, sleeping for a bit")
		if delay <= 0 {
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		}
	}
}

func (c *ClusterGlobalRateLimiter) getOrCreate(botHash uint64, botLimit uint) *leakybucket.Bucket {
	c.RLock()
	b, ok := c.globalBucketsMap[botHash]
	c.RUnlock()
	if !ok {
		c.Lock()
		// Check if it wasn't created while we didnt hold the exclusive lock
		b, ok = c.globalBucketsMap[botHash]
		if ok {
			c.Unlock()
			return b
		}

		globalBucket, _ := c.memStorage.Create(strconv.FormatUint(botHash, 10), botLimit, 1*time.Second)
		c.globalBucketsMap[botHash] = &globalBucket
		c.Unlock()
		return &globalBucket
	}
	return b
}

func (c *ClusterGlobalRateLimiter) FireGlobalRequest(ctx context.Context, addr string, botHash uint64, botLimit uint) error {
	globalReq, err := http.NewRequestWithContext(ctx, "GET", "http://"+addr+"/nirn/global", nil)
	if err != nil {
		return err
	}

	globalReq.Header.Set("bot-hash", strconv.FormatUint(botHash, 10))
	globalReq.Header.Set("bot-limit", strconv.FormatUint(uint64(botLimit), 10))

	// The node handling the request will only return if we grabbed a token or an error was thrown
	resp, err := client.Do(globalReq)
	logger.Trace("Got go-ahead for global")

	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != 200 {
		return errors.New("global request failed with status " + resp.Status)
	}

	return nil
}
