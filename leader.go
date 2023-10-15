package main

import (
	"fmt"
	"net"
	"os"
)

var (
	clientMap = make(map[int]net.Conn)
	nodeCount = 0
)

func readInput() {
	var input string
	fmt.Println("BEGGINING READ INPUT")
	// Read a single line of input
	for {
		_, err := fmt.Scan(&input)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Println("came here" + input)
		conn := clientMap[1]
		fmt.Println("Node Count", nodeCount)
		if nodeCount == 1 {

			_, _ = conn.Write([]byte(input))

		}
	}

}

func handleConnection(conn net.Conn) {
	//defer conn.Close()

	// Handle client connection here
	fmt.Println(clientMap)
	nodeCount++
	clientMap[nodeCount] = conn
	fmt.Println(nodeCount)

	// You can read and write data to the client using conn
	fmt.Printf("Accepted connection from %s\n", conn.RemoteAddr())
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run server.go <host:port>")
		os.Exit(1)
	}

	host := os.Args[1]

	// Create a listener
	listener, err := net.Listen("tcp", host)
	if err != nil {
		fmt.Printf("Error listening: %v\n", err)
		os.Exit(1)
	}
	//defer listener.Close()

	fmt.Printf("Server listening on %s\n", host)

	iterationNumber := 0

	for {
		fmt.Println("iteration #", iterationNumber)
		iterationNumber++
		// Accept incoming connections
		conn, err := listener.Accept()
		fmt.Print(conn)
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}

		// Handle the connection in a goroutine

		var values []net.Conn

		// Iterate through the map and append values to the array
		for _, value := range clientMap {
			values = append(values, value)
		}

		if !itemInSlice(conn, values) {
			go handleConnection(conn)
		}

		go readInput()

	}

}

func itemInSlice(item net.Conn, slice []net.Conn) bool {
	for _, value := range slice {
		if item == value {
			return true
		}
	}
	return false
}
