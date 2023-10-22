package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type users_row struct {
	Id    int
	Name  string
	Email string
}

var (
	followers []net.Conn
	conn_map  = make(map[int][]net.Conn)
)

// type users_row struct {
// 	id    int
// 	name  string
// 	email string
// }

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

	fmt.Printf("listening on : %s\n", host)

	conn_map[0] = []net.Conn{}
	conn_map[1] = []net.Conn{}

	go acceptFollowers(listener)

	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("enter a message: ")
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

		valList1, _ := conn_map[0]
		valList2, _ := conn_map[1]

		if len(valList2) < len(valList1) {
			valList2 = append(valList2, conn)
			conn_map[1] = valList2
		} else {
			valList1 = append(valList1, conn)
			conn_map[0] = valList1
		}

		fmt.Printf("accepted connection: %s", conn.RemoteAddr())
		go handleFollower(conn)
	}
}

func handleFollower(conn net.Conn) {
	defer conn.Close()

	for {
		buffer := make([]byte, 1028)
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Printf("Follower %s disconnected.\n", conn.RemoteAddr())
			removeFollower(conn)
			return
		}

		var receivedData []users_row
		//fmt.Println("buffer", buffer[:n])
		err = json.Unmarshal(buffer[:n], &receivedData)

		if err != nil {

			fmt.Println(err)
			return
		}

		fmt.Println(receivedData)
		// Process the received data
		for _, item := range receivedData {
			fmt.Printf("Received: ID=%d, UserName=%s, Email=%s\n", item.Id, item.Name, item.Email)
		}
		//msg := string(buffer)
		//fmt.Printf("\nReceived message from %s: %s", conn.RemoteAddr(), msg)
		//sendMessage(msg)
	}
}

func sendMessage(msg string) {
	fmt.Print("Leader sent: " + msg)

	queries := strings.Split(msg, ";")

	for _, query := range queries {
		isSelect := (strings.Contains(msg, "SELECT") || strings.Contains(msg, "select"))
		hasPrimaryKey := strings.Contains(msg, "ID") || strings.Contains(msg, "id")

		if isSelect {

			pattern := `[Ii][Dd]\s*(<=|>=|<|>|!=)\s*(\d+)`

			// Compile the regular expression
			re := regexp.MustCompile(pattern)

			// Find all matches in the input string
			matches := re.FindAllStringSubmatch(query, -1)

			hasEquality := len(matches) == 0

			if hasPrimaryKey && hasEquality {
				pattern := `[Ii][Dd]\s*=\s*(\d+)`

				// Compile the regular expression
				re := regexp.MustCompile(pattern)

				// Find all matches in the input string
				matches := re.FindAllStringSubmatch(query, -1)

				var id_str string
				for _, match := range matches {
					if len(match) > 1 {
						id_str = match[1]
					}
				}

				id, _ := strconv.Atoi(id_str)

				hashedId := hashID(id)

				// fmt.Println("connmap", conn_map, "hashedID", hashedId)

				connList, _ := conn_map[hashedId]

				// fmt.Println("connlist", connList)

				conn := connList[0]

				fmt.Println("conn", conn)
				_, err := conn.Write([]byte(query))
				if err != nil {
					fmt.Println(err)
					return
				}

			} else {
				for _, connList := range conn_map {
					fmt.Println("connList", connList)
					conn := connList[0]
					_, err := conn.Write([]byte(query))
					if err != nil {
						fmt.Println(err)
						return
					}
				}
			}

		} else {
			pattern := `\d+`

			re := regexp.MustCompile(pattern)

			// Find the first match in the input string
			match := re.FindString(query)
			pk, _ := strconv.Atoi(match)

			hashedId := hashID(pk)
			fmt.Println(pk)

			connList, _ := conn_map[hashedId]

			for _, conn := range connList {
				_, err := conn.Write([]byte(query))
				if err != nil {
					fmt.Println(err)
					return
				}
			}
		}

	}

	// for _, follower := range followers {
	// 	_, err := follower.Write([]byte(msg))
	// 	if err != nil {
	// 		fmt.Printf("Error sending message to %s: %v\n", follower.RemoteAddr(), err)
	// 		removeFollower(follower)
	// 	}
	// }
}

func hashID(id int) int {
	return id % 2
}

func removeFollower(follower net.Conn) {
	for i, conn := range followers {
		if conn == follower {
			followers = append(followers[:i], followers[i+1:]...)
			break
		}
	}
}
