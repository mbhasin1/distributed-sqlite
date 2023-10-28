package types

// // generic response structure from db
// type QueryResponse struct {
// 	UsersRow []UsersRow
// 	// TODO: add future types of response fields (joins, )
// 	Err string
// }

// // record structure from Users table
// type UsersRow struct {
// 	Id    int
// 	Name  string
// 	Email string
// }

// structure contains atrributes about a query
type Query struct {
	Query  string
	Type   string
	PKey   int // not 0 only if equality where condition on pkey is present
	Tables []string
	HasOr  bool
}
