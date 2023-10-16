package main

import (
	"fmt"
	"net"
	"os"
)

var clientList []net.Conn

func readInput() {
	var input string

	// Read a single line of input
	for {
		_, err := fmt.Scan(&input)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		for _, conn := range clientList {
			_, _ = conn.Write([]byte(input))
		}
	}

}

func readFromConnections() {
	fmt.Println("inR")
	for _, conn := range clientList {
		buffer := make([]byte, 1024)
		//conn.SetReadDeadline(time.Time{}) // setting infinite read deadline
		n, _ := conn.Read(buffer)
		response := string(buffer[:n])
		fmt.Println(response)
		fmt.Println("got to read")

	}

}

func handleClosedConnection() {
	for _, conn := range clientList {
		buf := make([]byte, 1, 1)
		_, err := conn.Read(buf)
		if err != nil {
			clientList = removeConnection(clientList, conn)
			fmt.Printf("Removed connection from %s\n", conn.RemoteAddr())
		}
	}
}

func removeConnection(connList []net.Conn, connToRemove net.Conn) []net.Conn {
	for idx, conn := range connList {
		if connToRemove == conn {
			return append(clientList[:idx], clientList[idx+1:]...)
		}
	}

	return connList
}

func handleConnection(conn net.Conn) {
	// defer conn.Close()
	clientList = append(clientList, conn)

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

	// defer listener.Close()

	fmt.Printf("Server listening on %s\n", host)

	for {
		// Accept incoming connections
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
		}

		go handleConnection(conn) // add new clients to client list

		go handleClosedConnection() // remove closed clients from client list

		go readInput() // continuously read terminal inputs

		go readFromConnections()

	}

}
