// package main

// import (
// 	"fmt"
// 	"strings"

// 	"distributed-sqlite/internal/parser"
// )

// func SendMessageToFollowers(msg string) {

// 	queries := strings.Split(msg, ";")

// 	for _, query := range queries {

// 		queryStruct, err := parser.ParseQuery(query)

// 		if err != nil {
// 			// write error back to leader, no need to send to followers!
// 		}

// 		if len(queryStruct.Tables) <= 1 && queryStruct.PKey != -1 && !queryStruct.HasOr {

// 			// send to one partition

// 			hashedId := hashID(queryStruct.PKey)

// 			connList, _ := connMap[hashedId]

// 			conn := connList[0]

// 			_, err := conn.Write([]byte(query))
// 			if err != nil {
// 				fmt.Println(err)
// 				return
// 			}

// 		} else {
// 			// write to both partitions

// 			for _, connList := range connMap {
// 				for _, conn := range connList {
// 					_, err := conn.Write([]byte(query))
// 					if err != nil {
// 						fmt.Println(err)
// 						return
// 					}
// 				}
// 			}

// 		}

// 	}
// }

// func hashID(id int) int {
// 	return id % 2
// }
