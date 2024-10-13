package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hashtable"
	"net"
)

const (
	SER_NIL = 0
	SER_ERR = 1
	SER_STR = 2
	SER_INT = 3
	SER_ARR = 4
)

var db *hashtable.HashTable

func StartServer() {
	db = hashtable.New()
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer listener.Close()
	fmt.Println("Server listening on :6379")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		cmd, err := readRequest(conn)
		if err != nil {
			fmt.Println("Error reading request:", err)
			return
		}
		response := handleCommand(cmd)
		err = writeResponse(conn, response)
		if err != nil {
			fmt.Println("Error writing response:", err)
			return
		}
	}
}

func readRequest(conn net.Conn) ([]string, error) {
	// Read length
	lenBuf := make([]byte, 4)
	_, err := conn.Read(lenBuf)
	if err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf)

	// Read content
	contentBuf := make([]byte, length)
	_, err = conn.Read(contentBuf)
	if err != nil {
		return nil, err
	}

	// Parse content
	return parseRequest(contentBuf)
}

func parseRequest(data []byte) ([]string, error) {
	if len(data) == 0 {
		return nil, errors.New("empty request")
	}
	numStrs := int(data[0])
	data = data[1:]
	result := make([]string, 0, numStrs)
	for i := 0; i < numStrs; i++ {
		if len(data) < 4 {
			return nil, errors.New("invalid request format")
		}
		strLen := binary.BigEndian.Uint32(data[:4])
		data = data[4:]
		if uint32(len(data)) < strLen {
			return nil, errors.New("invalid request format")
		}
		result = append(result, string(data[:strLen]))
		data = data[strLen:]
	}
	return result, nil
}

func handleCommand(cmd []string) []byte {
	if len(cmd) == 0 {
		return serializeError(1, "Empty command")
	}
	switch cmd[0] {
	case "get":
		return handleGet(cmd[1:])
	case "set":
		return handleSet(cmd[1:])
	case "del":
		return handleDel(cmd[1:])
	case "keys":
		return handleKeys()
	default:
		return serializeError(1, "Unknown command")
	}
}

func handleGet(args []string) []byte {
	if len(args) != 1 {
		return serializeError(1, "GET requires one argument")
	}
	value, found := db.Get(args[0])
	if !found {
		return serializeNil()
	}
	return serializeString(value.(string))
}

func handleSet(args []string) []byte {
	if len(args) != 2 {
		return serializeError(1, "SET requires two arguments")
	}
	db.Set(args[0], args[1])
	return serializeNil()
}

func handleDel(args []string) []byte {
	if len(args) != 1 {
		return serializeError(1, "DEL requires one argument")
	}
	deleted := db.Del(args[0])
	return serializeInt(boolToInt(deleted))
}

func handleKeys() []byte {
	keys := db.ListAll()
	return serializeArray(keys)
}

func writeResponse(conn net.Conn, response []byte) error {
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(response)))
	_, err := conn.Write(lenBuf)
	if err != nil {
		return err
	}
	_, err = conn.Write(response)
	return err
}

func serializeNil() []byte {
	return []byte{SER_NIL}
}

func serializeError(code int32, msg string) []byte {
	result := []byte{SER_ERR}
	result = append(result, int32ToBytes(code)...)
	result = append(result, uint32ToBytes(uint32(len(msg)))...)
	result = append(result, []byte(msg)...)
	return result
}

func serializeString(s string) []byte {
	result := []byte{SER_STR}
	result = append(result, uint32ToBytes(uint32(len(s)))...)
	result = append(result, []byte(s)...)
	return result
}

func serializeInt(n int64) []byte {
	result := []byte{SER_INT}
	result = append(result, int64ToBytes(n)...)
	return result
}

func serializeArray(arr []string) []byte {
	result := []byte{SER_ARR}
	result = append(result, uint32ToBytes(uint32(len(arr)))...)
	for _, s := range arr {
		result = append(result, serializeString(s[2:])...) // Remove "k: " prefix
	}
	return result
}

func int32ToBytes(n int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(n))
	return b
}

func uint32ToBytes(n uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, n)
	return b
}

func int64ToBytes(n int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return b
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func main() {
	StartServer()
}
