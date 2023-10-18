package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

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
	defer conn.Close()

	go receiveMessages(conn)
	sendMessages(conn)
}

func receiveMessages(conn net.Conn) {
	for {
		buffer := make([]byte, 1024)
		_, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Disconnected from leader.")
			os.Exit(0)
		}

		msg := string(buffer)
		fmt.Printf("Received message from leader: %s", msg)
	}
}

func sendMessages(conn net.Conn) {
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter a message: ")
		msg, _ := reader.ReadString('\n')
		_, err := conn.Write([]byte(msg))
		if err != nil {
			fmt.Println("Error sending message to leader:", err)
			os.Exit(1)
		}
	}
}
