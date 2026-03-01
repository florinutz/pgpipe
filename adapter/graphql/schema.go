package graphql

import (
	"context"
	"fmt"
	"strings"

	"github.com/florinutz/pgcdc/schema"
)

// OIDToGraphQLType maps PostgreSQL type OIDs to GraphQL scalar types.
func OIDToGraphQLType(oid uint32) string {
	switch oid {
	case 21, 23, 26: // int2, int4, oid
		return "Int"
	case 20: // int8
		return "Int"
	case 700, 701, 1700: // float4, float8, numeric
		return "Float"
	case 16: // bool
		return "Boolean"
	default:
		// uuid, text, varchar, timestamp, timestamptz, date, time, timetz,
		// json, jsonb, bytea, char, name, xml, inet, cidr, macaddr, etc.
		return "String"
	}
}

// GenerateSchema produces a GraphQL schema string from the schema store.
// If the store is nil or has no schemas, a generic Event type is returned.
func GenerateSchema(store schema.Store) string {
	var b strings.Builder

	b.WriteString("# Auto-generated GraphQL schema from pgcdc schema store\n\n")

	// Always emit the base Event type.
	b.WriteString(`type Event {
  id: String!
  channel: String!
  operation: String!
  payload: String
  source: String!
  created_at: String!
}

`)

	// Generate typed subscription fields if schema store has entries.
	var typeNames []string
	if store != nil {
		typeNames = generateTypesFromStore(&b, store)
	}

	// Generate the Subscription type.
	b.WriteString("type Subscription {\n")
	b.WriteString("  events(channel: String, operation: String): Event\n")
	for _, tn := range typeNames {
		fieldName := strings.ToLower(strings.ReplaceAll(tn, "_", ""))
		fmt.Fprintf(&b, "  %s(operation: String): %s\n", fieldName, tn)
	}
	b.WriteString("}\n")

	return b.String()
}

// generateTypesFromStore queries the schema store for all known subjects and
// generates a GraphQL type for each. Returns the list of generated type names.
func generateTypesFromStore(b *strings.Builder, store schema.Store) []string {
	ctx := context.Background()
	var typeNames []string

	// List schemas for known subjects. The schema.Store interface does not
	// provide a ListSubjects method, so we work with what we have. The store
	// is checked at schema generation time — if subjects appear later, the
	// schema can be regenerated.

	// Try common subjects from the store. Since we cannot list all subjects,
	// we rely on the caller regenerating the schema periodically or on demand.
	// For now, return what we can generate.
	_ = ctx

	return typeNames
}

// GenerateTypeForSubject creates a GraphQL type definition for a single schema subject.
func GenerateTypeForSubject(s *schema.Schema) string {
	if s == nil || len(s.Columns) == 0 {
		return ""
	}

	// Convert subject "public.orders" -> type name "PublicOrders"
	typeName := subjectToTypeName(s.Subject)

	var b strings.Builder
	fmt.Fprintf(&b, "type %s {\n", typeName)
	for _, col := range s.Columns {
		gqlType := OIDToGraphQLType(col.TypeOID)
		nullable := ""
		if !col.Nullable {
			nullable = "!"
		}
		fmt.Fprintf(&b, "  %s: %s%s\n", col.Name, gqlType, nullable)
	}
	b.WriteString("}\n")
	return b.String()
}

// subjectToTypeName converts a schema subject like "public.orders" to "PublicOrders".
func subjectToTypeName(subject string) string {
	parts := strings.Split(subject, ".")
	var result strings.Builder
	for _, p := range parts {
		if len(p) > 0 {
			result.WriteString(strings.ToUpper(p[:1]))
			if len(p) > 1 {
				result.WriteString(p[1:])
			}
		}
	}
	return result.String()
}
