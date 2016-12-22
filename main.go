package main

import (
	"encoding/json"
	"io/ioutil"
	"sync"
	"fmt"
	"net"
	"os"
	"strings"
)

var (
	gtm sync.Mutex
)

type Table struct {
	name string
	data map[string]string
	m sync.Mutex
}

func main() {
	ln, err := net.Listen("tcp", ":8000")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer conn.Close()
		go handleConnection(conn)
	}
}



func NewTable(file_name string, data map[string]string) *Table {
	return &Table{name: file_name, data: data,}
}

func ReadJSON(file_name string) *Table{
	key_val, err := ioutil.ReadFile("data/" + file_name)
	if err != nil {
		return nil 
	}
	var f map[string]string
    err = json.Unmarshal(key_val, &f)
    if err != nil {
		return nil 
	}
    t := NewTable(file_name, f)
	return t
}

func WriteJSON(table Table) {
	jsonData, _ := json.Marshal(table.data)
	f, err := os.Create("data/" + table.name)
    checkErr(err)
    defer f.Close()
    _, err = f.Write(jsonData)
    checkErr(err)
}

func checkErr(e error) {
	if e != nil {
		panic(e)
	}
}

func selectTable(name string) *Table {
	gtm.Lock()
	table := ReadJSON(name)
	gtm.Unlock()
	return table
}

func exit(c net.Conn) {
	c.Close()
}

func selectKeys(c net.Conn, query_split []string) {
	if len(query_split) == 2  {
		table := selectTable(query_split[0])
		if (table == nil) {
			c.Write([]byte(string("Unknown table\n")))
		} else {
			keys := make([]string, 0, len(table.data))
		    for k := range table.data {
		        keys = append(keys, k)
		    }
    		c.Write([]byte("[" + strings.Join(keys, ", ") + "]" + "\n"))
		}
	} else {
		c.Write([]byte(string("Unknown command\n")))
	}
}

func selectVal(c net.Conn, query_split []string) {
	if len(query_split) == 3  {
		table := selectTable(query_split[0])
		if (table == nil) {
			c.Write([]byte(string("Unknown table\n")))
		} else {
			value, ok := table.data[query_split[2]]
			if ok {
				c.Write([]byte(string(value + "\n")))
			} else {
				c.Write([]byte(string("key does not exist\n")))
			}
		}
	} else {
		c.Write([]byte(string("Unknown command\n")))
	}
}

func insertVal(c net.Conn, query_split []string) {
	if len(query_split) >= 4 {
		table := selectTable(query_split[0])
		if (table == nil) {
			table = NewTable(query_split[0], make(map[string]string))
		} 
		table.data[query_split[2]] = strings.Join(query_split[3: ], " ")
		table.m.Lock()
		WriteJSON(*table)

		table.m.Unlock()
		c.Write([]byte(string("OK\n")))
	} else {
		c.Write([]byte(string("Unknown command\n")))
	}
}

func updateVal(c net.Conn, query_split []string) {
	if len(query_split) >= 4 {
		table := selectTable(query_split[0])
		if (table == nil) {
			c.Write([]byte(string("Table doesn't exist \n")))
		} else {
		_, ok := table.data[query_split[2]]
			if ok {
			table.data[query_split[2]] = strings.Join(query_split[3: ], " ")
			table.m.Lock()
			WriteJSON(*table)
			table.m.Unlock()
			c.Write([]byte(string("OK\n")))
			} else {
				c.Write([]byte(string("key does not exist\n")))
			}
		}
	} else {
		c.Write([]byte(string("Unknown command\n")))
	}
}


func deleteKey(c net.Conn, query_split []string) {
	if len(query_split) == 3  {
		table := selectTable(query_split[0])
		if (table == nil) {
			c.Write([]byte(string("Unknown table\n")))
		} else {
			_, ok := table.data[query_split[2]]
			if ok {
				delete(table.data, query_split[2])
				table.m.Lock()
				WriteJSON(*table)
				table.m.Unlock()
				c.Write([]byte(string("OK\n")))
			} else {
				c.Write([]byte(string("key does not exist\n")))
			}
		}
	} else {
		c.Write([]byte(string("Unknown command\n")))
	}
}

func handleRequest(c net.Conn, query string) {
	query_split := strings.Fields(query)

	if len(query_split) >= 2 {
		switch strings.ToLower(query_split[1]) {
			case "insert":
				insertVal(c, query_split)
			case "update":
				updateVal(c, query_split)
			case "select":
				selectVal(c, query_split)
			case "delete":
				deleteKey(c, query_split)
			case "*":
				selectKeys(c, query_split)
			default:
				c.Write([]byte(string("Unknown command\n")))
		}
	} else if len(query_split) == 1 {
		switch strings.ToLower(query_split[0]) {
			case "quit":
				exit(c)
			default:
				c.Write([]byte(string("Unknown command\n")))
			}
	} else {
		c.Write([]byte(string("Unknown command\n")))
	}
}

func handleConnection(c net.Conn) {
	buf := make([]byte, 4096)
	for {
		n, err := c.Read(buf)
		if (err != nil) || (n == 0) {
			break
		} else {
			go handleRequest(c, string(buf[0:n]))
		}
	}
}

