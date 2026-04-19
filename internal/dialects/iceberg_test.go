package dialects

import (
	"strings"
	"testing"

	"github.com/pressly/goose/v3/database/dialect"
)

func TestIceberg_CreateTableUsesIcebergClause(t *testing.T) {
	got := NewIceberg().CreateTable("goose_db_version")
	if !strings.Contains(got, "USING iceberg") {
		t.Errorf("iceberg CreateTable missing USING iceberg clause: %s", got)
	}
	if !strings.Contains(got, "CREATE TABLE IF NOT EXISTS goose_db_version") {
		t.Errorf("iceberg CreateTable missing IF NOT EXISTS: %s", got)
	}
}

func TestDelta_CreateTableUsesDeltaClause(t *testing.T) {
	got := NewDelta().CreateTable("goose_db_version")
	if !strings.Contains(got, "USING DELTA") {
		t.Errorf("delta CreateTable missing USING DELTA clause: %s", got)
	}
}

// Every method except CreateTable should be byte-identical across
// the two dialects — the only format-dependent piece is the
// `USING <format>` clause on the version table.
func TestIcebergDelta_ShareQueryBodiesBeyondCreateTable(t *testing.T) {
	type methodCall struct {
		name string
		fn   func(dialect.Querier, string) string
	}
	table := "goose_db_version"
	cases := []methodCall{
		{"InsertVersion", func(q dialect.Querier, t string) string { return q.InsertVersion(t) }},
		{"DeleteVersion", func(q dialect.Querier, t string) string { return q.DeleteVersion(t) }},
		{"GetMigrationByVersion", func(q dialect.Querier, t string) string { return q.GetMigrationByVersion(t) }},
		{"ListMigrations", func(q dialect.Querier, t string) string { return q.ListMigrations(t) }},
		{"GetLatestVersion", func(q dialect.Querier, t string) string { return q.GetLatestVersion(t) }},
	}
	iceberg, delta := NewIceberg(), NewDelta()
	for _, c := range cases {
		if a, b := c.fn(iceberg, table), c.fn(delta, table); a != b {
			t.Errorf("%s diverges between iceberg and delta:\niceberg: %s\ndelta:   %s", c.name, a, b)
		}
	}
}

// The Spark Connect database/sql driver renders $N placeholders; any
// dialect that targets a Spark-SQL-speaking driver must emit $N and
// not `?`.
func TestSparkSQL_InsertUsesPositionalPlaceholders(t *testing.T) {
	got := NewIceberg().InsertVersion("goose_db_version")
	if !strings.Contains(got, "$1") || !strings.Contains(got, "$2") {
		t.Errorf("InsertVersion should use $N placeholders, got: %s", got)
	}
}

func TestSparkSQL_TableExistsWithSchemaQualifier(t *testing.T) {
	got := NewIceberg().TableExists("mydb.goose_db_version")
	if !strings.Contains(got, "table_schema = 'mydb'") {
		t.Errorf("TableExists should split schema from table: %s", got)
	}
	if !strings.Contains(got, "table_name = 'goose_db_version'") {
		t.Errorf("TableExists missing table_name clause: %s", got)
	}
}

func TestSparkSQL_TableExistsWithoutSchemaQualifier(t *testing.T) {
	got := NewIceberg().TableExists("goose_db_version")
	if strings.Contains(got, "table_schema") {
		t.Errorf("unqualified TableExists should not filter on schema: %s", got)
	}
	if !strings.Contains(got, "table_name = 'goose_db_version'") {
		t.Errorf("TableExists missing table_name filter: %s", got)
	}
}
