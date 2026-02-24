package transform

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/florinutz/pgcdc/event"
)

// MaskMode controls how a field value is masked.
type MaskMode string

const (
	MaskHash    MaskMode = "hash"    // SHA-256 hex digest
	MaskRedact  MaskMode = "redact"  // ***REDACTED***
	MaskPartial MaskMode = "partial" // keep first/last char, asterisks in middle
)

// MaskField pairs a payload field name with a masking mode.
type MaskField struct {
	Field string
	Mode  MaskMode
}

// Mask returns a transform that masks the specified payload fields.
// Missing fields are silently ignored. Values are converted to string via
// fmt.Sprintf before masking.
func Mask(fields ...MaskField) TransformFunc {
	return func(ev event.Event) (event.Event, error) {
		return withPayload(ev, func(m map[string]any) {
			for _, f := range fields {
				val, ok := m[f.Field]
				if !ok {
					continue
				}
				s := fmt.Sprintf("%v", val)
				switch f.Mode {
				case MaskHash:
					h := sha256.Sum256([]byte(s))
					m[f.Field] = fmt.Sprintf("%x", h)
				case MaskRedact:
					m[f.Field] = "***REDACTED***"
				case MaskPartial:
					m[f.Field] = partialMask(s)
				default:
					m[f.Field] = "***REDACTED***"
				}
			}
		})
	}
}

// partialMask keeps the first and last character, replacing the middle with
// asterisks. Strings shorter than 3 characters are fully redacted.
func partialMask(s string) string {
	runes := []rune(s)
	if len(runes) < 3 {
		return strings.Repeat("*", len(runes))
	}
	return string(runes[0]) + strings.Repeat("*", len(runes)-2) + string(runes[len(runes)-1])
}
