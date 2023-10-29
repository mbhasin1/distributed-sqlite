package parser

import (
	"distributed-sqlite/types"
	"regexp"
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func ParseQuery(query string) (*types.Query, error) {

	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}

	parsedQuery := &types.Query{}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		parsedQuery.Type = "select"
		parsedQuery.Tables = extractTableNamesFromSelect(stmt)
		whereClause := sqlparser.String(stmt.Where)
		parsedQuery.HasOr = hasOr(whereClause)
		parsedQuery.PKey = extractEqualityPKeyIfPresent(whereClause)
	case *sqlparser.Insert:
		parsedQuery.Type = "insert"
		rows := sqlparser.String(stmt.Rows)
		parsedQuery.PKey = extractPKeyFromInsert(rows)
	case *sqlparser.Update:
		parsedQuery.Type = "update"
		whereClause := sqlparser.String(stmt.Where)
		parsedQuery.HasOr = hasOr(whereClause)
		parsedQuery.PKey = extractEqualityPKeyIfPresent(whereClause)
	case *sqlparser.Delete:
		parsedQuery.Type = "delete"
		whereClause := sqlparser.String(stmt.Where)
		parsedQuery.HasOr = hasOr(whereClause)
		parsedQuery.PKey = extractEqualityPKeyIfPresent(whereClause)
	}

	return parsedQuery, nil

}

func extractTableNamesFromSelect(stmt *sqlparser.Select) []string {
	tableNames := []string{}

	for _, tableExpr := range stmt.From {
		switch tableExpr := tableExpr.(type) {
		case *sqlparser.AliasedTableExpr:
			tableNames = append(tableNames, extractTableNamesFromTableExpr(tableExpr))
		case *sqlparser.JoinTableExpr:
			tableNames = append(tableNames, extractTableNamesFromTableExpr(tableExpr.LeftExpr), extractTableNamesFromTableExpr(tableExpr.RightExpr))
		}
	}

	return tableNames
}

func extractPKeyFromInsert(values string) int {
	re := regexp.MustCompile(`\d+`)
	matches := re.FindString(values)

	if matches != "" {
		pKey, err := strconv.Atoi(matches)
		if err != nil {
			return -1
		}

		return pKey
	}

	return -1
}

func extractTableNamesFromTableExpr(tableExpr sqlparser.TableExpr) string {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch table := tableExpr.Expr.(type) {
		case sqlparser.TableName:
			return table.Name.String()
		}
	}

	return ""

}

func extractEqualityPKeyIfPresent(whereCond string) int {
	pKey := "id"

	re := regexp.MustCompile(pKey + ` *?= *?'([^']*)'`)
	matches := re.FindAllStringSubmatch(whereCond, -1)

	for _, match := range matches {
		if len(match) > 1 {
			pKey, err := strconv.Atoi(match[1])
			if err != nil {
				return -1
			}

			return pKey
		}
	}

	return -1

}

func hasOr(whereCond string) bool {
	return strings.Contains(whereCond, "or") || strings.Contains(whereCond, "OR")
}
