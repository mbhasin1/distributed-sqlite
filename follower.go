package main

import (
	"fmt"
	"net"
	"os"
	"time"
)

func readLeaderMessages() {

}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run client.go <server-host:server-port>")
		os.Exit(1)
	}

	serverAddr := os.Args[1]

	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		fmt.Printf("Error connecting to the server: %v\n", err)
		os.Exit(1)
	}
	//defer conn.Close()

	for {
		buffer := make([]byte, 1024)
		conn.SetReadDeadline(time.Time{}) // setting infinite read deadline
		n, err := conn.Read(buffer)
		response := string(buffer[:n])
		if err != nil {
			fmt.Println("Leader node closed connection")
			return
		}

		fmt.Println(response)

		conn.Write([]byte("yoyoyo"))
	}

}
