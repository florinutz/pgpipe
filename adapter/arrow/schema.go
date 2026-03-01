//go:build !no_arrow

package arrow

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/florinutz/pgcdc/schema"
)

// OIDToArrowType maps a PostgreSQL type OID to the corresponding Arrow data type.
func OIDToArrowType(oid uint32) arrow.DataType {
	switch oid {
	case 21: // int2
		return arrow.PrimitiveTypes.Int16
	case 23: // int4
		return arrow.PrimitiveTypes.Int32
	case 20: // int8
		return arrow.PrimitiveTypes.Int64
	case 700: // float4
		return arrow.PrimitiveTypes.Float32
	case 701: // float8
		return arrow.PrimitiveTypes.Float64
	case 1700: // numeric
		return arrow.PrimitiveTypes.Float64
	case 16: // bool
		return arrow.FixedWidthTypes.Boolean
	case 25, 1043, 1042, 19: // text, varchar, bpchar, name
		return arrow.BinaryTypes.String
	case 2950: // uuid
		return arrow.BinaryTypes.String
	case 1114: // timestamp
		return &arrow.TimestampType{Unit: arrow.Microsecond}
	case 1184: // timestamptz
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	case 1082: // date
		return arrow.FixedWidthTypes.Date32
	case 1083: // time
		return arrow.FixedWidthTypes.Time64us
	case 1266: // timetz
		return arrow.FixedWidthTypes.Time64us
	case 3802, 114: // jsonb, json
		return arrow.BinaryTypes.String
	case 17: // bytea
		return arrow.BinaryTypes.Binary
	default:
		return arrow.BinaryTypes.String
	}
}

// BuildSchemaFromStore creates an Arrow schema from the schema store for a subject.
// Returns nil if the store is nil or the subject is not found.
func BuildSchemaFromStore(store schema.Store, subject string) *arrow.Schema {
	if store == nil {
		return nil
	}
	s, err := store.Latest(context.Background(), subject)
	if err != nil || s == nil {
		return nil
	}

	fields := make([]arrow.Field, len(s.Columns))
	for i, col := range s.Columns {
		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     OIDToArrowType(col.TypeOID),
			Nullable: col.Nullable,
		}
	}
	return arrow.NewSchema(fields, nil)
}

// DefaultEventSchema returns the default Arrow schema for CDC events when
// no schema store is available.
func DefaultEventSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "channel", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "operation", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "created_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
	}, nil)
}
