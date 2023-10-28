package main

import (
	"bufio"
	"distributed-sqlite/internal/parser"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
)

var (
	connMap = make(map[int][]net.Conn) // map of <partition id, pool of active participants>
)

// record structure from Users table
type UsersRow struct {
	Id    int
	Name  string
	Email string
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

		valList1, _ := connMap[0]
		valList2, _ := connMap[1]

		if len(valList2) < len(valList1) {
			valList2 = append(valList2, conn)
			connMap[1] = valList2
		} else {
			valList1 = append(valList1, conn)
			connMap[0] = valList1
		}

		fmt.Printf("Accepted connection: %s. \n", conn.RemoteAddr())
		go readFromFollower(conn)
	}
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
			fmt.Printf("Received: ID=%d, UserName=%s, Email=%s\n", item.Id, item.Name, item.Email)
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

	queries := strings.Split(msg, ";")

	// iterate throuugh all queries except last (empty) query
	for i := 0; i < len(queries)-1; i++ {

		query := queries[i]

		queryStruct, err := parser.ParseQuery(query)

		if err != nil {
			// write error back to leader, no need to send to followers!
		}

		fmt.Println(queryStruct)

		if len(queryStruct.Tables) <= 1 && queryStruct.PKey != -1 && !queryStruct.HasOr {

			// send to one partition

			hashedId := hashID(queryStruct.PKey)

			connList, _ := connMap[hashedId]

			conn := connList[0]

			_, err := conn.Write([]byte(query))
			if err != nil {
				fmt.Println(err)
				return
			}

		} else {

			// send to both partitions

			for _, connList := range connMap {
				for _, conn := range connList {
					_, err := conn.Write([]byte(query))
					if err != nil {
						fmt.Println(err)
						return
					}
				}
			}

		}

	}
}

func hashID(id int) int {
	return id % 2
}
