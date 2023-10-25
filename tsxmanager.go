package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

func SendMessageToFollowers(msg string) {

	queries := strings.Split(msg, ";")

	for _, query := range queries {

		// parse query here

		if isSelectQuery {

			// select query with equality on primary key - send to one primary node
			if hasPrimaryKey && hasEquality {
				pattern := `[Ii][Dd]\s*=\s*(\d+)`

				re := regexp.MustCompile(pattern)

				matches := re.FindAllStringSubmatch(query, -1)

				var id_str string
				for _, match := range matches {
					if len(match) > 1 {
						id_str = match[1]
					}
				}

				id, _ := strconv.Atoi(id_str)

				hashedId := hashID(id)

				connList, _ := connMap[hashedId]

				conn := connList[0]

				_, err := conn.Write([]byte(query))
				if err != nil {
					fmt.Println(err)
					return
				}

			} else {
				// select query with non-equality on primary key - send to both primary nodes
				for _, connList := range connMap {
					conn := connList[0]
					_, err := conn.Write([]byte(query))
					if err != nil {
						fmt.Println(err)
						return
					}
				}
			}

		} else {
			// insert, update, delete
			pattern := `\d+`

			re := regexp.MustCompile(pattern)

			match := re.FindString(query)

			pk, _ := strconv.Atoi(match)

			hashedId := hashID(pk)

			connList, _ := connMap[hashedId]

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

func hashID(id int) int {
	return id % 2
}
