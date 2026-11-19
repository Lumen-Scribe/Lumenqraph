// --- begin doc example ---
package webhooks

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
)

const toleranceSecs = 300

// VerifyWebhookSignature checks an X-Lumenqraph-Signature header of the form
// "t=<unix>,v1=<hex>[,v1=<hex>...]". rawBody must be the exact HTTP body bytes
// and now is the current Unix time in seconds.
func VerifyWebhookSignature(rawBody []byte, signatureHeader, secret string, now int64) bool {
	var timestampText string
	var haveTimestamp bool
	var signatures []string

	for _, part := range strings.Split(signatureHeader, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		switch key {
		case "t":
			timestampText, haveTimestamp = value, true
		case "v1":
			signatures = append(signatures, value)
		}
	}
	if !haveTimestamp || len(signatures) == 0 {
		return false
	}
	timestamp, err := strconv.ParseInt(timestampText, 10, 64)
	if err != nil {
		return false
	}

	// Reject stale or future-dated deliveries (replay protection)
	diff := now - timestamp
	if diff < 0 {
		diff = -diff
	}
	if diff > toleranceSecs {
		return false
	}

	// HMAC-SHA256 over "{t}.{body}", using the timestamp text exactly as sent
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestampText + "."))
	mac.Write(rawBody)
	expected := mac.Sum(nil)

	// Accept if any v1 matches (supports secret rotation); hmac.Equal is constant time
	for _, sig := range signatures {
		candidate, err := hex.DecodeString(sig)
		if err == nil && hmac.Equal(candidate, expected) {
			return true
		}
	}
	return false
}
// --- end doc example ---
