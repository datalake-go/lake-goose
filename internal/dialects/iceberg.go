package dialects

import (
	"fmt"

	"github.com/pressly/goose/v3/database/dialect"
)

// NewIceberg returns a goose dialect that stores the goose_db_version
// bookkeeping table as an Apache Iceberg table. The dialect is
// transport-agnostic: any database/sql driver that can execute Spark
// SQL (Spark Connect, Databricks SQL, JDBC bridge, etc.) can use it.
// Application-level migrations are free to create Delta, Parquet, or
// Iceberg tables via their own `CREATE TABLE ... USING <format>` —
// the dialect only dictates the format of its own version table.
func NewIceberg() dialect.QuerierExtender {
	return &sparksqlDialect{versionTableFormat: "iceberg"}
}

// NewDelta returns a goose dialect that stores the goose_db_version
// bookkeeping table as a Delta table. See [NewIceberg] for why
// format lives on the dialect rather than on the transport driver.
func NewDelta() dialect.QuerierExtender {
	return &sparksqlDialect{versionTableFormat: "DELTA"}
}

// sparksqlDialect is the shared Spark-SQL-flavoured Querier. The
// only split between Iceberg and Delta is the `USING <format>`
// clause on the version-table DDL; every other query is identical
// because Spark SQL's INSERT / DELETE / SELECT surface is the same
// across both formats.
type sparksqlDialect struct {
	// versionTableFormat is the `USING <format>` value for the
	// goose_db_version CREATE statement. "iceberg" or "DELTA".
	// Iceberg's docs document lowercase "iceberg"; Delta's examples
	// use uppercase "DELTA". Both are case-insensitive in Spark, but
	// we pass through each format's canonical spelling.
	versionTableFormat string
}

var _ dialect.QuerierExtender = (*sparksqlDialect)(nil)

// CreateTable emits a Spark SQL CREATE TABLE IF NOT EXISTS for the
// goose_db_version bookkeeping table. Three columns, no primary key
// (Spark SQL doesn't enforce PKs), no auto-increment (goose
// generates version IDs itself).
func (d *sparksqlDialect) CreateTable(tableName string) string {
	q := `CREATE TABLE IF NOT EXISTS %s (
	version_id BIGINT,
	is_applied BOOLEAN,
	tstamp TIMESTAMP
) USING %s`
	return fmt.Sprintf(q, tableName, d.versionTableFormat)
}

// InsertVersion uses $N placeholders because the Spark Connect
// database/sql driver renders $N inline; see that driver's render.go
// for the rationale (Spark Connect's protocol-level parameter
// binding isn't round-trip-stable across every supported Spark
// version). Any other Spark-SQL-speaking driver that wants to use
// these dialects needs to accept $N placeholders too.
func (d *sparksqlDialect) InsertVersion(tableName string) string {
	return fmt.Sprintf(`INSERT INTO %s (version_id, is_applied) VALUES ($1, $2)`, tableName)
}

func (d *sparksqlDialect) DeleteVersion(tableName string) string {
	return fmt.Sprintf(`DELETE FROM %s WHERE version_id = $1`, tableName)
}

// GetMigrationByVersion returns the most recent tstamp for a given
// version. A rolled-back-and-reapplied migration leaves multiple
// rows; newest wins.
func (d *sparksqlDialect) GetMigrationByVersion(tableName string) string {
	return fmt.Sprintf(
		`SELECT tstamp, is_applied FROM %s WHERE version_id = $1 ORDER BY tstamp DESC LIMIT 1`,
		tableName,
	)
}

// ListMigrations orders by version_id because Spark tables don't
// carry the auto-increment id column Postgres uses. Goose's version
// IDs are timestamps so chronological = version-id-ordered.
func (d *sparksqlDialect) ListMigrations(tableName string) string {
	return fmt.Sprintf(`SELECT version_id, is_applied FROM %s ORDER BY version_id DESC`, tableName)
}

func (d *sparksqlDialect) GetLatestVersion(tableName string) string {
	return fmt.Sprintf(`SELECT MAX(version_id) FROM %s`, tableName)
}

// TableExists reads Spark's INFORMATION_SCHEMA. Iceberg's Spark
// extensions and Delta's catalog provider both populate it, so this
// one query works for both dialects.
func (d *sparksqlDialect) TableExists(tableName string) string {
	schema, table := parseTableIdentifier(tableName)
	if schema != "" {
		return fmt.Sprintf(
			`SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'`,
			schema, table,
		)
	}
	return fmt.Sprintf(
		`SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_name = '%s'`,
		table,
	)
}
