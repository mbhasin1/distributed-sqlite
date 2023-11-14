package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// record structure from Users table
type UsersRow struct {
	Id       int
	Name     string
	Email    string
	PrepResp string
	DbNumber int
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

	dbNumberString := string(filepath[2])

	dbNumber, _ := strconv.Atoi(dbNumberString)

	initUserRow := UsersRow{
		DbNumber: dbNumber,
	}

	sendMessages(conn, []UsersRow{initUserRow})

	defer db.Close()
	go receiveMessages(conn, db)

	for { // wait for go routines
	}

}

func openDB(filepath string) *sql.DB {
	fmt.Println("Connection active")

	db, err := sql.Open("sqlite3", filepath)
	if err != nil {
		fmt.Println(err)

	}

	return db
}

func exec_query(db *sql.DB, query string, conn net.Conn) {
	startsWithP := strings.HasPrefix(query, "p")

	if startsWithP {
		fmt.Println("Running in prepare phase...")

		//strip the "prepare"
		query = query[7:]

		tx, _ := db.Begin()

		defer tx.Rollback() // The rollback will be ignored if the tx has been committed later in the function.

		_, err := tx.Exec(query)

		if err != nil {
			fmt.Println("Error:", err)
			user_row := UsersRow{
				PrepResp: "Fail",
			}

			fmt.Println("Prepare response: ", user_row.PrepResp)
			sendMessages(conn, []UsersRow{user_row})
			return
		}

		user_row := UsersRow{
			PrepResp: "Pass",
		}

		fmt.Println("Prepare response: ", user_row.PrepResp)
		sendMessages(conn, []UsersRow{user_row})

	} else {
		fmt.Println("Running in commit phase...")

		//strip the "commit"
		query = query[6:]
		tx, _ := db.Begin()

		defer tx.Rollback() // The rollback will be ignored if the tx has been committed later in the function.

		_, _ = tx.Exec(query)

		_ = tx.Commit()

		user_row := UsersRow{
			PrepResp: "Pass",
		}

		fmt.Println("Commit succeeded!")
		sendMessages(conn, []UsersRow{user_row})
	}

	return
}

func query_sql(db *sql.DB, query string, conn net.Conn) {

	startsWithP := strings.HasPrefix(query, "p")

	if startsWithP {
		fmt.Println("Running in prepare phase...")

		//strip the "prepare"
		query = query[7:]

		tx, _ := db.Begin()

		defer tx.Rollback() // The rollback will be ignored if the tx has been committed later in the function.

		_, err := tx.Query(query)
		if err != nil {
			fmt.Println("Error:", err)
			user_row := UsersRow{
				PrepResp: "Fail",
			}
			fmt.Println("Prepare response: ", user_row.PrepResp)
			sendMessages(conn, []UsersRow{user_row})
			return
		}

		user_row := UsersRow{
			PrepResp: "Pass",
		}

		fmt.Println("Prepare response: ", user_row.PrepResp)
		sendMessages(conn, []UsersRow{user_row})

	} else {
		fmt.Println("Running in commit phase...")

		//strip the "commit"
		query = query[6:]
		tx, _ := db.Begin()

		defer tx.Rollback() // The rollback will be ignored if the tx has been committed later in the function.

		rows, err := tx.Query(query)
		if err != nil {
			fmt.Println(err)
			return
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
				return
			}

			user_row := UsersRow{
				Id:       id,
				Name:     username,
				Email:    email,
				PrepResp: "Pass",
			}

			ret_rows = append(ret_rows, user_row)

			fmt.Println("Query result:", ret_rows)
		}

		sendMessages(conn, ret_rows)
		fmt.Println("Sent result to leader!")
		return
	}
	return
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
		fmt.Printf("Received message from leader: %s\n", msg)

		// if query contains select send to query_sql
		isSelect := (strings.Contains(msg, "SELECT") || strings.Contains(msg, "select"))

		if isSelect {
			query_sql(db, msg, conn)
		} else {
			exec_query(db, msg, conn)
		}
	}
}

// write query result back to leader
func sendMessages(conn net.Conn, rows []UsersRow) {
	jsonData, err := json.Marshal(rows)

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
