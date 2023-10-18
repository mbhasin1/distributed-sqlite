package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

var followers []net.Conn

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

	fmt.Printf("Server listening on %s\n", host)
	if err != nil {
		fmt.Println("Error listening:", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Printf("Leader server is listening on : %s\n", host)

	go acceptFollowers(listener)

	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Leader, enter a message: ")
		msg, _ := reader.ReadString('\n')
		sendMessage(msg)
	}
}

func acceptFollowers(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		followers = append(followers, conn)

		go handleFollower(conn)
	}
}

func handleFollower(conn net.Conn) {
	defer conn.Close()

	for {
		buffer := make([]byte, 1024)
		_, err := conn.Read(buffer)
		if err != nil {
			fmt.Printf("Follower %s disconnected.\n", conn.RemoteAddr())
			removeFollower(conn)
			return
		}

		msg := string(buffer)
		fmt.Printf("Received message from %s: %s", conn.RemoteAddr(), msg)
		//sendMessage(msg)
	}
}

func sendMessage(msg string) {
	fmt.Print("Leader sent: " + msg)

	for _, follower := range followers {
		_, err := follower.Write([]byte(msg))
		if err != nil {
			fmt.Printf("Error sending message to %s: %v\n", follower.RemoteAddr(), err)
			removeFollower(follower)
		}
	}
}

func removeFollower(follower net.Conn) {
	for i, conn := range followers {
		if conn == follower {
			followers = append(followers[:i], followers[i+1:]...)
			break
		}
	}
}
