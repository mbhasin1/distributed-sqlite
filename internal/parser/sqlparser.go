package parser

import (
	"fmt"

	"github.com/marianogappa/sqlparser"
)

// functionalities:
// if select query, send back FROM tables, COLUMNS to return, parition key (if present), equality or range
// insert query, send back FROM table, partition key
// delete query, send back FROM table, partition key (if present), equality or range
// update query, send back FROM table, partition key
func ParseQuery(query string) (err error) {

	parsedQuery, err := sqlparser.Parse(query)
	if err != nil {
		return err
	}

	fmt.Println(parsedQuery)

	return nil

}
