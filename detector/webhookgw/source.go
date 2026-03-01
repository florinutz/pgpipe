package webhookgw

// Source configures an external webhook source.
type Source struct {
	Name            string // source identifier (e.g., "stripe", "github")
	Secret          string // HMAC signing secret
	SignatureHeader string // HTTP header containing the signature (e.g., "Stripe-Signature", "X-Hub-Signature-256")
	ChannelPrefix   string // channel prefix for events (default: "pgcdc:{name}")
}
