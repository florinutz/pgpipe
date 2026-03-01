package webhookgw

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ValidateSignature validates an HMAC-SHA256 signature from the given header value.
// It auto-detects the format based on header name or content:
//   - Stripe: "Stripe-Signature" header with "t=<timestamp>,v1=<hex>" format.
//     Computes HMAC-SHA256 of "<timestamp>.<body>".
//   - GitHub: "X-Hub-Signature-256" header with "sha256=<hex>" format.
//     Computes HMAC-SHA256 of body.
//   - Generic: Raw hex HMAC-SHA256 of body.
func ValidateSignature(headerValue, body, secret, headerName string) error {
	if secret == "" {
		return nil
	}
	if headerValue == "" {
		return fmt.Errorf("missing signature header %q", headerName)
	}

	// Stripe format: t=<ts>,v1=<sig>
	if strings.EqualFold(headerName, "Stripe-Signature") || strings.HasPrefix(headerValue, "t=") {
		return validateStripe(headerValue, body, secret)
	}

	// GitHub format: sha256=<hex>
	if strings.EqualFold(headerName, "X-Hub-Signature-256") || strings.HasPrefix(headerValue, "sha256=") {
		return validateGitHub(headerValue, body, secret)
	}

	// Generic: raw hex HMAC-SHA256.
	return validateGeneric(headerValue, body, secret)
}

// stripeTimestampTolerance is the maximum age of a Stripe webhook signature
// timestamp before it is rejected to mitigate replay attacks.
const stripeTimestampTolerance = 5 * time.Minute

func validateStripe(headerValue, body, secret string) error {
	var timestamp, sig string
	for _, part := range strings.Split(headerValue, ",") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "t":
			timestamp = kv[1]
		case "v1":
			sig = kv[1]
		}
	}
	if timestamp == "" || sig == "" {
		return fmt.Errorf("invalid Stripe signature format: missing t or v1")
	}

	// Reject timestamps older than tolerance to mitigate replay attacks.
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid Stripe timestamp: %w", err)
	}
	if time.Since(time.Unix(ts, 0)) > stripeTimestampTolerance {
		return fmt.Errorf("stripe signature timestamp too old")
	}

	signedPayload := timestamp + "." + body
	expected := computeHMACSHA256(signedPayload, secret)

	sigBytes, err := hex.DecodeString(sig)
	if err != nil {
		return fmt.Errorf("decode signature hex: %w", err)
	}
	if !hmac.Equal(sigBytes, expected) {
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

func validateGitHub(headerValue, body, secret string) error {
	sig := strings.TrimPrefix(headerValue, "sha256=")
	expected := computeHMACSHA256(body, secret)

	sigBytes, err := hex.DecodeString(sig)
	if err != nil {
		return fmt.Errorf("decode signature hex: %w", err)
	}
	if !hmac.Equal(sigBytes, expected) {
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

func validateGeneric(headerValue, body, secret string) error {
	expected := computeHMACSHA256(body, secret)

	sigBytes, err := hex.DecodeString(headerValue)
	if err != nil {
		return fmt.Errorf("decode signature hex: %w", err)
	}
	if !hmac.Equal(sigBytes, expected) {
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

func computeHMACSHA256(message, secret string) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(message))
	return mac.Sum(nil)
}
