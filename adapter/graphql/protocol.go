package graphql

import "encoding/json"

// MessageType represents the graphql-transport-ws protocol message types.
// See: https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md
type MessageType string

const (
	MsgConnectionInit MessageType = "connection_init"
	MsgConnectionAck  MessageType = "connection_ack"
	MsgPing           MessageType = "ping"
	MsgPong           MessageType = "pong"
	MsgSubscribe      MessageType = "subscribe"
	MsgNext           MessageType = "next"
	MsgError          MessageType = "error"
	MsgComplete       MessageType = "complete"
)

// Subprotocol is the WebSocket subprotocol for graphql-transport-ws.
const Subprotocol = "graphql-transport-ws"

// Message is the wire format for graphql-transport-ws protocol messages.
type Message struct {
	ID      string          `json:"id,omitempty"`
	Type    MessageType     `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// SubscribePayload is the payload for a subscribe message.
type SubscribePayload struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

// nextPayload wraps the data sent in a "next" message.
type nextPayload struct {
	Data any `json:"data"`
}

// errorPayload wraps an error sent in an "error" message.
type errorPayload struct {
	Message string `json:"message"`
}

// parseSubscription extracts the target type name and field selections from
// a GraphQL subscription query. This is a minimal parser — no full GraphQL
// parsing is needed. It looks for patterns like:
//
//	subscription { events { id channel payload } }
//	subscription { events(where: {...}) { id channel } }
//
// Returns the subscription field name (e.g. "events"), selected fields (nil = all),
// and any where variables.
func parseSubscription(payload SubscribePayload) (field string, selections []string, variables map[string]any) {
	variables = payload.Variables
	field, selections = parseQueryFields(payload.Query)
	if field == "" {
		field = "events"
	}
	return field, selections, variables
}

// parseQueryFields extracts the root field name and its sub-selections from a
// subscription query string. Handles basic patterns:
//
//	subscription { events { id channel } }
//	subscription Name { events { id channel } }
//	{ events { id channel } }
func parseQueryFields(query string) (rootField string, fields []string) {
	// Tokenize: split on whitespace and braces.
	tokens := tokenize(query)
	if len(tokens) == 0 {
		return "", nil
	}

	// Skip "subscription" keyword and optional operation name.
	i := 0
	if i < len(tokens) && tokens[i] == "subscription" {
		i++
		// Skip optional operation name (an identifier before '{').
		if i < len(tokens) && tokens[i] != "{" {
			i++
		}
	}

	// Skip opening '{'.
	if i < len(tokens) && tokens[i] == "{" {
		i++
	}

	// The root field name.
	if i >= len(tokens) {
		return "", nil
	}
	rootField = tokens[i]
	i++

	// Skip optional arguments (everything between '(' and ')').
	if i < len(tokens) && tokens[i] == "(" {
		depth := 1
		i++
		for i < len(tokens) && depth > 0 {
			switch tokens[i] {
			case "(":
				depth++
			case ")":
				depth--
			}
			i++
		}
	}

	// Collect sub-selections inside '{' ... '}'.
	if i < len(tokens) && tokens[i] == "{" {
		i++
		depth := 1
		for i < len(tokens) && depth > 0 {
			t := tokens[i]
			switch t {
			case "{":
				depth++
			case "}":
				depth--
			default:
				if depth == 1 {
					fields = append(fields, t)
				}
			}
			i++
		}
	}

	return rootField, fields
}

// tokenize splits a GraphQL query into tokens (identifiers, braces, parens, colons).
func tokenize(s string) []string {
	var tokens []string
	i := 0
	for i < len(s) {
		ch := s[i]
		switch ch {
		case ' ', '\t', '\n', '\r', ',':
			i++
		case '{', '}', '(', ')', ':':
			tokens = append(tokens, string(ch))
			i++
		case '"':
			// Skip string literals.
			j := i + 1
			for j < len(s) && s[j] != '"' {
				if s[j] == '\\' {
					j++
				}
				j++
			}
			if j < len(s) {
				j++ // closing quote
			}
			tokens = append(tokens, s[i:j])
			i = j
		case '#':
			// Skip comments.
			for i < len(s) && s[i] != '\n' {
				i++
			}
		default:
			// Identifier or keyword.
			j := i
			for j < len(s) && !isSep(s[j]) {
				j++
			}
			tokens = append(tokens, s[i:j])
			i = j
		}
	}
	return tokens
}

func isSep(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ',' ||
		c == '{' || c == '}' || c == '(' || c == ')' || c == ':' || c == '#'
}
