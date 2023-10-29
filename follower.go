package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// record structure from Users table
type UsersRow struct {
	Id        int
	Name      string
	Email     string
	VoteCount int
}

// structure contains atrributes about a query
type Query struct {
	Query  string
	Type   string
	PKey   int // not 0 only if equality where condition on pkey is present
	Tables []string
	HasOr  bool
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: go run follower.go <server-host:server-port> <path-to-db-file.db>")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
	filepath := os.Args[2]

	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		fmt.Printf("Error connecting to the server: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	db := openDB(filepath)
	defer db.Close()
	go receiveMessages(conn, db)

	// wait for go routines
	for {
	}

}

func openDB(filepath string) *sql.DB {
	fmt.Print("in open\n")

	db, err := sql.Open("sqlite3", filepath)
	if err != nil {
		fmt.Println(err)

	}

	return db
}

func exec_query(db *sql.DB, query string, conn net.Conn) string {
	startsWithP := strings.HasPrefix(query, "p")

	if startsWithP {
		fmt.Println("it did started with p")
		//strip the p
		query = query[1:]
		tx, err := db.Begin()
		if err != nil {
			// call sendmessage
			user_row := UsersRow{
				VoteCount: -1,
			}
			sendMessages(conn, []UsersRow{user_row})
			return ""
		}
		defer tx.Rollback() // The rollback will be ignored if the tx has been committed later in the function.

		fmt.Println("made it past db begin")

		_, err = tx.Exec(query)
		if err != nil {
			fmt.Println(err)
			user_row := UsersRow{
				VoteCount: -1,
			}
			sendMessages(conn, []UsersRow{user_row})
			fmt.Println("failed votecount: ", user_row.VoteCount)
			return ""
		}

		user_row := UsersRow{
			VoteCount: 1,
		}

		fmt.Println("VoteCount: ", user_row.VoteCount)
		sendMessages(conn, []UsersRow{user_row})

	} else {
		fmt.Println("it did started with c")
		//strip the c
		query = query[1:]
		tx, err := db.Begin()
		if err != nil {
			// call sendmessage
			user_row := UsersRow{
				VoteCount: -1,
			}
			sendMessages(conn, []UsersRow{user_row})
			return ""
		}
		defer tx.Rollback() // The rollback will be ignored if the tx has been committed later in the function.

		fmt.Println("made it past db begin")

		_, err = tx.Exec(query)
		if err != nil {
			fmt.Println(err)
			user_row := UsersRow{
				VoteCount: -1,
			}
			sendMessages(conn, []UsersRow{user_row})
			fmt.Println("failed votecount: ", user_row.VoteCount)
			return ""
		}

		err = tx.Commit()
		if err != nil {
			fmt.Println("did it come here?")
			user_row := UsersRow{
				VoteCount: -1,
			}
			sendMessages(conn, []UsersRow{user_row})
		}

		user_row := UsersRow{
			VoteCount: 1,
		}

		fmt.Println("VoteCount: ", user_row.VoteCount)
		sendMessages(conn, []UsersRow{user_row})
	}

	return ""

	// _, err := db.Exec(query)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return "Fail"
	// }
	// return "Pass"

}

func query_sql(db *sql.DB, query string, conn net.Conn) string {

	startsWithP := strings.HasPrefix(query, "p")

	if startsWithP {
		fmt.Println("it did started with p")
		//strip the p
		query = query[1:]
		tx, err := db.Begin()
		if err != nil {
			// call sendmessage
			user_row := UsersRow{
				VoteCount: -1,
			}
			sendMessages(conn, []UsersRow{user_row})
			return ""
		}
		defer tx.Rollback() // The rollback will be ignored if the tx has been committed later in the function.

		fmt.Println("made it past db begin")

		_, err = tx.Query(query)
		if err != nil {
			fmt.Println(err)
			user_row := UsersRow{
				VoteCount: -1,
			}
			sendMessages(conn, []UsersRow{user_row})
			fmt.Println("failed votecount: ", user_row.VoteCount)
			return ""
		}

		user_row := UsersRow{
			VoteCount: 1,
		}

		fmt.Println("VoteCount: ", user_row.VoteCount)
		sendMessages(conn, []UsersRow{user_row})

	} else {
		fmt.Println("it did started with c")
		//strip the c
		query = query[1:]
		tx, err := db.Begin()
		if err != nil {
			// call sendmessage
			user_row := UsersRow{
				VoteCount: -1,
			}
			sendMessages(conn, []UsersRow{user_row})
			return ""
		}
		// defer tx.Rollback() // The rollback will be ignored if the tx has been committed later in the function.

		rows, err := tx.Query(query)
		if err != nil {
			fmt.Println(err)
			return ""
		}
		defer rows.Close()

		ret_rows := []UsersRow{}

		for rows.Next() {
			var id int
			var username string
			var email string
			err := rows.Scan(&id, &username, &email)
			if err != nil {
				fmt.Println(err)
				return ""
			}

			user_row := UsersRow{
				Id:        id,
				Name:      username,
				Email:     email,
				VoteCount: 1,
			}

			ret_rows = append(ret_rows, user_row)

			fmt.Println(ret_rows)
		}

		sendMessages(conn, ret_rows)
		return ""
	}
	return ""
}

func receiveMessages(conn net.Conn, db *sql.DB) {
	for {
		buffer := make([]byte, 1024)
		_, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Disconnected from leader.")
			os.Exit(0)
		}

		msg := string(buffer)
		fmt.Printf("Received message from leader: %s", msg)

		// if query contains select send to query_sql
		isSelect := (strings.Contains(msg, "SELECT") || strings.Contains(msg, "select"))

		if isSelect {
			query_sql(db, msg, conn)
			// sendMessages(conn, rows)
		} else {
			exec_query(db, msg, conn)

		}
	}
}

func sendMessages(conn net.Conn, rows []UsersRow) {
	//fmt.Println((rows))
	jsonData, err := json.Marshal(rows)
	//fmt.Println((jsonData))
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = conn.Write(jsonData)
	if err != nil {
		fmt.Println(err)
		return
	}
}
