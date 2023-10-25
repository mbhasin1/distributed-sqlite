package types

// generic response structure from db
type QueryResponse struct {
	UsersRow []UsersRow
	// TODO: add future types of response fields (joins, )
	Err string
}

// record structure from Users table
type UsersRow struct {
	Id    int
	Name  string
	Email string
}
