package lib

import (
	"encoding/base64"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MajorUnknown      = "unk"
	MajorChannels     = "channels"
	MajorGuilds       = "guilds"
	MajorWebhooks     = "webhooks"
	MajorInvites      = "invites"
	MajorInteractions = "interactions"
)

func IsSnowflake(str string) bool {
	l := len(str)
	if l < 17 || l > 20 {
		return false
	}
	for _, d := range str {
		if d < '0' || d > '9' {
			return false
		}
	}
	return true
}

func IsNumericInput(str string) bool {
	for _, d := range str {
		if d < '0' || d > '9' {
			return false
		}
	}
	return true
}

func GetMetricsPath(route string) string {
	return MetricsPathFromBucket(GetOptimisticBucketPath(route, ""))
}

func MetricsPathFromBucket(route string) string {
	if strings.HasPrefix(route, "/invite/!") {
		return "/invite/!"
	}

	var path strings.Builder
	parts := strings.Split(route, "/")
	for _, part := range parts {
		if part == "" {
			continue
		}
		if IsNumericInput(part) {
			path.WriteString("/!")
		} else {
			path.WriteByte('/')
			path.WriteString(part)
		}
	}

	result := path.String()
	if !utf8.ValidString(result) {
		logger.Warn("Non utf-8 path detected, Prometheus only supports utf-8, invalid runes will be replaced with @ in metrics. Path: " + result)
		result = strings.ToValidUTF8(result, "@")
	}

	return result
}

func GetOptimisticBucketPath(url string, method string) string {
	bucket := strings.Builder{}
	bucket.WriteByte('/')
	cleanUrl := strings.SplitN(url, "?", 2)[0]
	if versioned := strings.TrimPrefix(cleanUrl, "/api/v"); versioned != cleanUrl {
		if slash := strings.IndexByte(versioned, '/'); slash >= 0 && IsNumericInput(versioned[:slash]) {
			cleanUrl = versioned[slash+1:]
		} else {
			cleanUrl = strings.TrimPrefix(cleanUrl, "/api/")
		}
	} else if unversioned := strings.TrimPrefix(cleanUrl, "/api/"); unversioned != cleanUrl {
		cleanUrl = unversioned
	} else {
		cleanUrl = strings.TrimPrefix(cleanUrl, "/")
	}

	parts := strings.Split(cleanUrl, "/")
	numParts := len(parts)

	if numParts <= 1 {
		return cleanUrl
	}

	currMajor := MajorUnknown
	// ! stands for any replaceable id
	switch parts[0] {
	case MajorChannels:
		bucket.WriteString(MajorChannels)
		bucket.WriteByte('/')
		bucket.WriteString(parts[1])
		currMajor = MajorChannels
	case MajorInvites:
		bucket.WriteString(MajorInvites)
		bucket.WriteString("/!")
		currMajor = MajorInvites
	case MajorGuilds:
		fallthrough
	case MajorInteractions:
		if numParts == 4 && parts[3] == "callback" {
			return "/" + MajorInteractions + "/" + parts[1] + "/!/callback"
		}
		fallthrough
	case MajorWebhooks:
		fallthrough
	default:
		bucket.WriteString(parts[0])
		bucket.WriteByte('/')
		bucket.WriteString(parts[1])
		currMajor = parts[0]
	}

	if numParts == 2 {
		return bucket.String()
	}

	// At this point, the major + id part is already accounted for
	// In this loop, we only need to strip all remaining snowflakes, emoji names and webhook tokens(optional)
	for idx, part := range parts[2:] {
		if IsSnowflake(part) {
			// Custom rule for direct message DELETE only (not reactions etc.)
			if currMajor == MajorChannels && parts[1+idx] == "messages" && method == "DELETE" && idx == len(parts[2:])-1 {
				createdAt, _ := GetSnowflakeCreatedAt(part)
				if createdAt.Before(time.Now().Add(-1 * 14 * 24 * time.Hour)) {
					bucket.WriteString("/!14dmsg")
				} else if createdAt.After(time.Now().Add(-1 * 10 * time.Second)) {
					bucket.WriteString("/!10smsg")
				}
				continue
			}
			bucket.WriteString("/!")
		} else {
			if currMajor == MajorChannels && part == "reactions" {
				// reaction put/delete fall under a different bucket from other reaction endpoints
				if method == "PUT" || method == "DELETE" {
					bucket.WriteString("/reactions/!modify")
					break
				}
				//All other reaction endpoints falls under the same bucket, so it's irrelevant if the user
				//is passing userid, emoji, etc.
				bucket.WriteString("/reactions/!/!")
				//Reactions can only be followed by emoji/userid combo, since we don't care, break
				break
			}

			// Strip webhook tokens, or extract interaction ID
			if len(part) >= 64 {
				// aW50ZXJhY3Rpb246 is base64 for "interaction:"
				if !strings.HasPrefix(part, "aW50ZXJhY3Rpb246") {
					bucket.WriteString("/!")
					continue
				}

				var interactionId string

				// fix padding
				if i := len(part) % 4; i != 0 {
					part += strings.Repeat("=", 4-i)
				}

				decodedPart, err := base64.StdEncoding.DecodeString(part)
				if err != nil {
					interactionId = "Unknown"
				} else {
					_, interactionId, _ = strings.Cut(string(decodedPart), ":")
					interactionId, _, _ = strings.Cut(interactionId, ":")
					if interactionId == "" {
						interactionId = "Unknown"
					}
				}

				bucket.WriteByte('/')
				bucket.WriteString(interactionId)
				continue
			}
			bucket.WriteByte('/')
			bucket.WriteString(part)
		}
	}

	return bucket.String()
}
