package main

import (
	"bufio"
	"distributed-sqlite/internal/parser"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	connMap     = make(map[int][]net.Conn) // map of <partition id, pool of active participants>
	dbMap       = make(map[net.Conn]int)   // map of <connection, db file number>
	allPrepared = true
)

// generic structure from Users table
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

	if len(os.Args) != 2 {
		fmt.Println("Usage: go run leader.go <host:port>")
		os.Exit(1)
	}

	hostName := os.Args[1]

	// Create a listener
	listener, err := net.Listen("tcp", hostName)
	if err != nil {
		fmt.Printf("Error listening: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Server listening on %s\n", hostName)

	defer listener.Close()

	// initialize connMap with no active connections
	connMap[0] = []net.Conn{}
	connMap[1] = []net.Conn{}

	go acceptFollowers(listener) // accept new followers and register them

	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("enter a message: ")
		msg, _ := reader.ReadString('\n')
		sendMessageToTsxMngr(msg)
	}
}

func acceptFollowers(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		fmt.Printf("Accepted connection: %s. \n", conn.RemoteAddr())
		go readFromFollower(conn)
	}
}

func addConnToConnMap(dbNumber int, conn net.Conn) {
	dbMap[conn] = dbNumber
	if dbNumber == 1 || dbNumber == 3 {
		connList := connMap[0]
		if len(connList) == 1 { // if there's already a node in the partition, make it consistent with it
			makeConsistent(dbMap[connList[0]], dbNumber)
		}
		connMap[0] = append(connMap[0], conn)

	} else {
		connList := connMap[1]
		if len(connList) == 1 {
			makeConsistent(dbMap[connList[0]], dbNumber)

		}
		connMap[1] = append(connMap[1], conn)
	}
}

func makeConsistent(existingDbNumber int, newDbNumber int) {
	existingDbName := "db" + strconv.Itoa(existingDbNumber) + ".db"
	newDbName := "db" + strconv.Itoa(newDbNumber) + ".db"
	duplicate(existingDbName, newDbName)

}

func duplicate(existingDbName string, newDbName string) {

	// Opening the original file
	existingDbFile, err := os.Open(existingDbName)
	if err != nil {
		panic(err)
	}
	defer existingDbFile.Close()

	// Creating the duplicate file
	newDbFile, err := os.Create(newDbName)
	if err != nil {
		panic(err)
	}
	defer newDbFile.Close()

	// Copying the contents of the original file to the duplicate file
	_, err = io.Copy(newDbFile, existingDbFile)
	if err != nil {
		panic(err)
	}

	// The file has been successfully duplicated
	fmt.Println("New connection consistent with primary node")
}

func readFromFollower(conn net.Conn) {
	defer conn.Close()

	for {
		buffer := make([]byte, 1028)
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Printf("Follower %s disconnected.\n", conn.RemoteAddr())
			removeFollower(conn)
			return
		}

		var receivedData []UsersRow

		err = json.Unmarshal(buffer[:n], &receivedData)

		if err != nil {
			fmt.Println(err)
			return
		}

		// Process the received data
		for _, item := range receivedData {
			if item.PrepResp == "Fail" { // is a prepare failure message
				fmt.Println("Prepare response: Fail")
				allPrepared = false
			} else {
				if item.DbNumber != 0 { // is a register connection message
					addConnToConnMap(item.DbNumber, conn)
				} else if item.Name != "" { // is query response mssage
					//output to out file here
					fmt.Printf("Received: ID=%d, UserName=%s, Email=%s\n", item.Id, item.Name, item.Email)
				} else {
					fmt.Println("Prepare response: Pass")
				}
			}
		}
	}
}

func sendMessageToTsxMngr(msg string) {
	fmt.Print("Leader sent: " + msg)
	SendMessageToFollowers(msg)

}

func removeFollower(follower net.Conn) {
	for _, connList := range connMap {
		for i, conn := range connList {
			if conn == follower {
				connList = append(connList[:i], connList[i+1:]...)
				connMap[i] = connList
				break
			}
		}
	}
}

func SendMessageToFollowers(msg string) {
	allPrepared = true
	queries := strings.Split(msg, ";")

	toWriteConnList := []net.Conn{}

	// PREPARE phase - iterate through all queries except last (empty) query
	for i := 0; i < len(queries)-1; i++ {

		query := queries[i]
		queryStruct, err := parser.ParseQuery(query)

		if err != nil { // Return error and do not send query to followers
			fmt.Println("Error with query:", err)
			return
		}

		toWriteConnList = getToWriteConnList(Query(*queryStruct))

		for _, conn := range toWriteConnList {
			fmt.Printf("Polling connection %v ... \n", conn.RemoteAddr())
			prepare(query, conn)
			time.Sleep(1 * time.Second)
		}
	}

	// COMMIT phase - iterate through all queries except last (empty) query
	if allPrepared {
		for i := 0; i < len(queries)-1; i++ {

			query := queries[i]

			for _, conn := range toWriteConnList {
				fmt.Printf("Committing transaction to connection %v ... \n", conn.RemoteAddr())
				commit(query, conn)
				time.Sleep(1 * time.Second)
			}
		}
	} else { // 1+ queries failed in prepare phase
		fmt.Println("One or more transactions failed the prepare phase!")
	}
}

func getToWriteConnList(queryStruct Query) []net.Conn {
	toWriteConnList := []net.Conn{}

	if len(queryStruct.Tables) <= 1 && queryStruct.PKey != -1 && !queryStruct.HasOr { // send to one partition

		hashedId := hashID(queryStruct.PKey)

		connList, _ := connMap[hashedId]

		// send all valid select queries to first partition
		if queryStruct.Type == "select" || queryStruct.Type == "SELECT" {
			toWriteConnList = append(toWriteConnList, connList[0])
		} else {
			for _, conn := range connList {
				toWriteConnList = append(toWriteConnList, conn)
			}
		}

	} else { // send to both partitions

		for _, connList := range connMap {
			if queryStruct.Type == "select" || queryStruct.Type == "SELECT" {
				toWriteConnList = append(toWriteConnList, connList[0])
			} else {
				for _, conn := range connList {
					toWriteConnList = append(toWriteConnList, conn)
				}
			}
		}
	}

	return toWriteConnList
}

func prepare(query string, conn net.Conn) {
	query = "prepare" + " " + query

	_, err := conn.Write([]byte(query))
	if err != nil {
		fmt.Println(err)
		return
	}
}

func commit(query string, conn net.Conn) {
	query = "commit" + " " + query

	_, err := conn.Write([]byte(query))
	if err != nil {
		fmt.Println(err)
		return
	}
}

func hashID(id int) int {
	return id % 2
}
