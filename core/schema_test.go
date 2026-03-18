package core

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/dosco/graphjin/core/v3/internal/qcode"
	"github.com/dosco/graphjin/core/v3/internal/sdata"
)

func TestCreateSchema(t *testing.T) {
	var buf bytes.Buffer

	di1 := sdata.GetTestDBInfo()
	if err := writeSchema(di1, &buf); err != nil {
		t.Fatal(err)
	}

	ds, err := qcode.ParseSchema(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	di2 := sdata.NewDBInfo(ds.Type,
		ds.Version,
		ds.Schema,
		"",
		ds.Columns,
		ds.Functions,
		nil)

	if di1.Hash() != di2.Hash() {
		t.Fatal(fmt.Errorf("schema hashes do not match: expected %d got %d",
			di1.Hash(), di2.Hash()))
	}
}

// TestSchemaRoundTripRelationshipFields verifies that FKeySchema, FKeyTable, and FKeyCol
// round-trip correctly through writeSchema and ParseSchema, including for composite-FK-style
// columns (same table, same FKeyTable, different FKeyCol).
func TestSchemaRoundTripRelationshipFields(t *testing.T) {
	// Build DBInfo with composite-FK-style columns: two columns (tenant_id, organization_id)
	// both reference tenant_organizations but different target columns.
	cols := []sdata.DBColumn{
		{Schema: "public", Table: "tenant_organizations", Name: "tenant_id", Type: "bigint", NotNull: true, PrimaryKey: true},
		{Schema: "public", Table: "tenant_organizations", Name: "organization_id", Type: "bigint", NotNull: true, PrimaryKey: true},
		{Schema: "public", Table: "purchase_orders", Name: "id", Type: "bigint", NotNull: true, PrimaryKey: true},
		{Schema: "public", Table: "purchase_orders", Name: "tenant_id", Type: "bigint", NotNull: true, FKeySchema: "public", FKeyTable: "tenant_organizations", FKeyCol: "tenant_id"},
		{Schema: "public", Table: "purchase_orders", Name: "organization_id", Type: "bigint", NotNull: true, FKeySchema: "public", FKeyTable: "tenant_organizations", FKeyCol: "organization_id"},
	}
	di1 := sdata.NewDBInfo("postgres", 150000, "public", "db", cols, nil, nil)

	// Write schema to buffer and parse it back.
	var buf bytes.Buffer
	if err := writeSchema(di1, &buf); err != nil {
		t.Fatal(err)
	}

	ds, err := qcode.ParseSchema(buf.Bytes())
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}

	di2 := sdata.NewDBInfo(ds.Type, ds.Version, ds.Schema, "", ds.Columns, ds.Functions, nil)

	// Assert every column's relationship fields match after round-trip.
	for _, t1 := range di1.Tables {
		_, err := di2.GetTable(t1.Schema, t1.Name)
		if err != nil {
			t.Fatalf("table %s.%s not found after round-trip: %v", t1.Schema, t1.Name, err)
		}
		for _, c1 := range t1.Columns {
			c2, err := di2.GetColumn(t1.Schema, t1.Name, c1.Name)
			if err != nil {
				t.Fatalf("column %s.%s.%s not found after round-trip: %v", t1.Schema, t1.Name, c1.Name, err)
			}
			if c1.FKeySchema != c2.FKeySchema || c1.FKeyTable != c2.FKeyTable || c1.FKeyCol != c2.FKeyCol {
				t.Errorf("relationship round-trip mismatch for %s.%s.%s: got FKeySchema=%q FKeyTable=%q FKeyCol=%q, want FKeySchema=%q FKeyTable=%q FKeyCol=%q",
					t1.Schema, t1.Name, c1.Name,
					c2.FKeySchema, c2.FKeyTable, c2.FKeyCol,
					c1.FKeySchema, c1.FKeyTable, c1.FKeyCol)
			}
		}
	}
}
