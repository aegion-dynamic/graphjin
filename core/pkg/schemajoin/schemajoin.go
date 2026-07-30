// Package schemajoin exposes internal schema discovery and FK pathfinding
// for external use (internal/sdata cannot be imported from other modules).
package schemajoin

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/dosco/graphjin/core/v3/internal/sdata"
)

// Re-exported types from sdata for callers outside the core module.
type (
	DBInfo     = sdata.DBInfo
	DBSchema   = sdata.DBSchema
	DBTable    = sdata.DBTable
	DBColumn   = sdata.DBColumn
	DBFunction = sdata.DBFunction
	TPath      = sdata.TPath
	DBRel      = sdata.DBRel
	RelType    = sdata.RelType
)

// PathToRel converts one hop of a path to DBRel.
var PathToRel = sdata.PathToRel

// Supported rel-types for switch comparisons.
const (
	RelOneToOne  = sdata.RelOneToOne
	RelOneToMany = sdata.RelOneToMany
)

// GetDBInfo loads table/column/FK metadata from the database.
func GetDBInfo(db *sql.DB, dbType string, blockList []string) (*DBInfo, error) {
	return sdata.GetDBInfo(db, dbType, blockList)
}

// NewDBInfo builds a DBInfo from raw column data. Useful for tests that need to
// construct schemas in memory without a real database.
func NewDBInfo(dbType string, dbVersion int, dbSchema string, dbName string, cols []DBColumn, funcs []DBFunction, blockList []string) *DBInfo {
	return sdata.NewDBInfo(dbType, dbVersion, dbSchema, dbName, cols, funcs, blockList)
}

// NewDBSchema builds the relationship graph used by FindPath.
func NewDBSchema(info *DBInfo, aliases map[string][]string) (*DBSchema, error) {
	if aliases == nil {
		aliases = map[string][]string{}
	}
	return sdata.NewDBSchema(info, aliases)
}

// PathKeyForTable returns the key GraphJin uses in edgesIndex for pathfinding.
// When schema is non-empty it returns "schema:table" to support cross-schema scenarios.
func PathKeyForTable(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + ":" + table
}

// FindPathChildToParent finds a join path from child table to parent table.
func FindPathChildToParent(schema *DBSchema, childTable, parentTable, through string) ([]TPath, error) {
	return schema.FindPath(childTable, parentTable, through)
}

// ColumnExists reports whether a column exists on a table in the loaded DBInfo.
func ColumnExists(info *DBInfo, schema, table, column string) bool {
	if schema == "" {
		schema = info.Schema
	}
	_, err := info.GetColumn(schema, table, column)
	return err == nil
}

// ValidateQualifiedName checks that schema.table exists in DBInfo.
func ValidateQualifiedName(info *DBInfo, schema, table string) error {
	if schema == "" {
		schema = info.Schema
	}
	_, err := info.GetTable(schema, table)
	if err != nil {
		return fmt.Errorf("table not found in schema: %w", err)
	}
	return nil
}

// FormatQualifiedTable returns "schema"."table" for SQL identifiers.
func FormatQualifiedTable(schema, table string) string {
	return `"` + strings.ReplaceAll(schema, `"`, `""`) + `"."` + strings.ReplaceAll(table, `"`, `""`) + `"`
}
