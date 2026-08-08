package psql

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/dosco/graphjin/core/v3/internal/qcode"
)

func (c *compilerContext) alias(alias string) {
	c.w.WriteString(` AS `)
	c.quoted(alias)
}

func (c *compilerContext) aliasWithID(alias string, id int32) {
	c.w.WriteString(` AS `)
	c.quoted(alias + "_" + strconv.Itoa(int(id)))
}

func (c *compilerContext) colWithTableID(table string, id int32, col string) {
	if id >= 0 {
		c.quoted(table + "_" + strconv.Itoa(int(id)))
	} else {
		c.quoted(table)
	}
	c.w.WriteString(`.`)
	c.quoted(col)
}

// table renders a table reference with optional schema and alias
func (c *compilerContext) table(schema, table string, alias bool) {
	renderAlias := func(t string) {
		c.w.WriteString(` AS `)
		c.quoted(t)
	}

	sel := &qcode.Select{
		Table:  table,
		Schema: schema,
	}

	c.renderTable(sel)

	if alias {
		renderAlias(table)
	}
}

func (c *compilerContext) colWithTable(table, col string) {
	c.quoted(table)
	c.w.WriteString(`.`)
	c.quoted(col)
}

func (c *compilerContext) quoted(identifier string) {
	switch c.ct {
	case "mysql":
		c.w.WriteByte('`')
		c.w.WriteString(identifier)
		c.w.WriteByte('`')
	default:
		c.w.WriteByte('"')
		c.w.WriteString(identifier)
		c.w.WriteByte('"')
	}
}

func (c *compilerContext) squoted(identifier string) {
	c.w.WriteByte('\'')
	c.w.WriteString(identifier)
	c.w.WriteByte('\'')
}

func int32String(w *bytes.Buffer, val int32) {
	w.WriteString(strconv.FormatInt(int64(val), 10))
}

// QuoteIdent properly quotes a SQL identifier, handling existing quotes
func QuoteIdent(identifier string) string {
	// Clean and normalize the identifier
	clean := func(s string) string {
		// Remove any existing quotes
		s = strings.Trim(s, `"`)
		s = strings.Trim(s, "`")
		// Escape existing double quotes
		return strings.ReplaceAll(s, `"`, `""`)
	}

	// Skip empty identifiers
	if identifier == "" {
		return ""
	}

	return `"` + clean(identifier) + `"`
}