package task



import (
	"testing"
	"net"
	"math/rand"
	"sync"
	"io"
	"bytes"
	"os"
	"fmt"
	//"time"
)


func TestStreamIntegrity(t *testing.T) {
	l, err := net.Listen("unix", "/tmp/out.sock")
	if err != nil {
		panic("Init sock " + err.Error())
	}
	defer l.Close()
	defer os.Remove("/tmp/out.sock")
	original_data := make([]byte, 5000000)
	rand.Read(original_data)
	var sock net.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		sock, err = l.Accept()
		if err != nil {
			panic("Accept sock " + err.Error())
		}
	}(&wg)
	s, err := createStream("/tmp/in.sock", "/tmp/out.sock", ".", 500, 0, 0)
	if err != nil {
		panic("Create stream " + err.Error())
	}
	wg.Wait()
	defer sock.Close()
	fmt.Println(s.GetState())
	c, err := net.Dial("unix", "/tmp/in.sock")
	if err != nil {
		panic("Open sock " + err.Error())
	}
	_, err = c.Write(original_data)
	if err != nil {
		panic("Write error " + err.Error())
	}
	c.Close()
	err = nil
	var recv_data []byte
	next := true
	for next {
		data := make([]byte, 1024)
		count, err := sock.Read(data)
		if err != nil {
			if err == io.EOF {
				next = false
			} else {
				panic("Read error " + err.Error())
			}
		}
		recv_data = append(recv_data, data[:count]...)
		//time.Sleep(10 * time.Microsecond)
	}
	fmt.Println(len(recv_data))
	if !bytes.Equal(original_data, recv_data) {
		t.Error("Integrity test fail original data is corrupted")
	}
	fmt.Println(s.GetState())
}
