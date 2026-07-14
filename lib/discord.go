package lib

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var client *http.Client

var contextTimeout = 5 * time.Second

var globalOverrideMap = make(map[string]uint)

var disableRestLimitDetection = false

type contextKey string

const metricsPathContextKey contextKey = "metricsPath"

type BotGatewayResponse struct {
	SessionStartLimit map[string]int `json:"session_start_limit"`
}

type BotUserResponse struct {
	Id       string `json:"id"`
	Username string `json:"username"`
	Discrim  string `json:"discriminator"`
}

func createTransport(ip string, disableHttp2 bool) http.RoundTripper {
	var transport http.Transport
	if ip == "" {
		// http.DefaultTransport options
		transport = http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          1000,
			MaxIdleConnsPerHost:   256,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
	} else {
		addr, err := net.ResolveTCPAddr("tcp", ip+":0")

		if err != nil {
			panic(err)
		}

		dialer := &net.Dialer{
			LocalAddr: addr,
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}

		dialContext := func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dialer.Dial(network, addr)
			return conn, err
		}

		transport = http.Transport{
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          1000,
			MaxIdleConnsPerHost:   256,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 2 * time.Second,
			DialContext:           dialContext,
			ResponseHeaderTimeout: 0,
		}
	}

	if disableHttp2 {
		transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		transport.ForceAttemptHTTP2 = false
	}

	return &transport
}

func parseGlobalOverrides(overrides string) {
	// Format: "<bot_id>:<bot_global_limit>,<bot_id>:<bot_global_limit>

	if overrides == "" {
		return
	}

	overrideList := strings.Split(overrides, ",")
	for _, override := range overrideList {
		opts := strings.SplitN(override, ":", 2)
		if len(opts) != 2 {
			panic("Invalid bot global ratelimit overrides")
		}

		limit, err := strconv.ParseInt(opts[1], 10, 32)

		if err != nil || limit <= 0 || opts[0] == "" {
			panic("Failed to parse global ratelimit overrides")
		}

		globalOverrideMap[opts[0]] = uint(limit)
	}
}

func ConfigureDiscordHTTPClient(ip string, timeout time.Duration, disableHttp2 bool, globalOverrides string, disableRestDetection bool) {
	if timeout <= 0 {
		panic("REQUEST_TIMEOUT must be greater than zero")
	}
	transport := createTransport(ip, disableHttp2)
	client = &http.Client{
		Transport: transport,
		Timeout:   90 * time.Second,
	}

	contextTimeout = timeout

	disableRestLimitDetection = disableRestDetection

	globalOverrideMap = make(map[string]uint)
	parseGlobalOverrides(globalOverrides)
}

func GetBotGlobalLimit(token string, user *BotUserResponse) (uint, error) {
	if token == "" {
		return math.MaxUint32, nil
	}

	if user != nil {
		limitOverride, ok := globalOverrideMap[user.Id]
		if ok {
			return limitOverride, nil
		}
	}

	if HasAuthPrefix(token, "Bearer") {
		return 50, nil
	}

	if HasAuthPrefix(token, "Basic") {
		return 50, nil
	}

	if disableRestLimitDetection {
		return 50, nil
	}

	bot, err := doDiscordReq(context.Background(), "/api/v9/gateway/bot", "GET", nil, map[string][]string{"Authorization": {token}}, "")

	if err != nil {
		return 0, err
	}
	defer bot.Body.Close()

	switch {
	case bot.StatusCode == 401:
		// In case a 401 is encountered, we return math.MaxUint32 to allow requests through to fail fast
		return math.MaxUint32, errors.New("invalid token - nirn-proxy")
	case bot.StatusCode == 429:
		return 0, errors.New("429 on gateway/bot")
	case bot.StatusCode == 500:
		return 0, errors.New("500 on gateway/bot")
	}

	body, err := io.ReadAll(bot.Body)
	if err != nil {
		return 0, err
	}

	var s BotGatewayResponse

	err = json.Unmarshal(body, &s)
	if err != nil {
		return 0, err
	}

	concurrency := s.SessionStartLimit["max_concurrency"]
	if concurrency <= 0 {
		return 0, errors.New("gateway/bot response missing max_concurrency")
	}

	if concurrency == 1 {
		return 50, nil
	} else {
		if 25*concurrency > 500 {
			return uint(25 * concurrency), nil
		}
		return 500, nil
	}
}

func GetBotUser(token string) (*BotUserResponse, error) {
	if token == "" {
		return nil, errors.New("no token provided")
	}

	bot, err := doDiscordReq(context.Background(), "/api/v9/users/@me", "GET", nil, map[string][]string{"Authorization": {token}}, "")

	if err != nil {
		return nil, err
	}
	defer bot.Body.Close()

	switch {
	case bot.StatusCode == 401:
		// gateway/bot performs the canonical invalid-token check below.
	case bot.StatusCode == 429:
		return nil, errors.New("429 on users/@me")
	case bot.StatusCode >= 400:
		return nil, fmt.Errorf("users/@me failed with status %s", bot.Status)
	}

	body, err := io.ReadAll(bot.Body)
	if err != nil {
		return nil, err
	}

	var s BotUserResponse

	err = json.Unmarshal(body, &s)
	if err != nil {
		return nil, err
	}

	return &s, nil
}

func doDiscordReq(ctx context.Context, path string, method string, body io.ReadCloser, header http.Header, query string) (*http.Response, error) {
	discordReq, err := http.NewRequestWithContext(ctx, method, "https://discord.com"+path+"?"+query, body)
	if err != nil {
		return nil, err
	}

	discordReq.Header = header
	startTime := time.Now()
	discordResp, err := client.Do(discordReq)

	identifier := ctx.Value("identifier")
	if identifier == nil {
		// Queues always have an identifier, if there's none in the context, we called the method from outside a queue
		identifier = "Internal"
	}

	if err == nil {
		route, _ := ctx.Value(metricsPathContextKey).(string)
		if route == "" {
			route = GetMetricsPath(path)
		}
		status := discordResp.Status
		method := discordResp.Request.Method
		elapsed := time.Since(startTime).Seconds()

		if discordResp.StatusCode == 429 {
			if discordResp.Header.Get("x-ratelimit-scope") == "shared" {
				status = "429 Shared"
			}
		}

		RequestHistogram.WithLabelValues(method, status, route, identifier.(string)).Observe(elapsed)
	}
	return discordResp, err
}
