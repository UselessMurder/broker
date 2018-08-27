package task



import (
	"testing"
	"net"
	"math/rand"
	"sync"
	"io"
	"bytes"
	"os"
	"strconv"
)


func TestTaskState(t *testing.T) {
	var targets [][]string
	var listeners []*net.Listener
	for i := 0; i < 10; i++ {
		target := make([]string, 2)
		target[0] = "/tmp/out.sock" + strconv.Itoa(i)
		target[1] = "/tmp/in.sock" + strconv.Itoa(i)
		targets = append(targets, target)
	}
	for _, value := range targets {
		err := os.RemoveAll(value[1])
		if err != nil {
			panic("Remove socket " + value[1] + " " + err.Error())
		}
		l, err := net.Listen("unix", value[1])
		if err != nil {
			panic("Init sock " + err.Error())
		}
		listeners = append(listeners, &l)
	}
	task, err := CreateTask(targets, ".", 0, 50, 500)
	if err != nil {
		panic("Init task error " + err.Error())
	}
	_, state := task.GetState()
	if state != "performing" {
		t.Error("State of pending task is invalid")
	}
	task.Close()
	_, state = task.GetState()
	if state != "breaked" {
		t.Error("Try to break task failed")
	}
        task, err = CreateTask(targets, ".", 0, 50, 500)
	for key, value := range targets {
		(*listeners[key]).Close()
		os.RemoveAll(value[1])
		conn, err := net.Dial("unix", value[0])
		if err != nil {
			panic("Open sock " + err.Error())
		}
		_, err = conn.Write([]byte{1,2,3})
		if err != nil {
			panic("Write sock " + err.Error())
		}
		defer conn.Close()
	}
	task.Wait()
	_, state = task.GetState()
	if state != "failed" {
		t.Error("Try to corrupt task failed")
	}
	listeners = listeners[:0]
	for _, value := range targets {
		err := os.RemoveAll(value[1])
		if err != nil {
			panic("Remove socket " + value[1] + " " + err.Error())
		}
		l, err := net.Listen("unix", value[1])
		if err != nil {
			panic("Init sock " + err.Error())
		}
		listeners = append(listeners, &l)
	}
        task, err = CreateTask(targets, ".", 0, 50, 500)
	for _, value := range targets {
		conn, err := net.Dial("unix", value[0])
		if err != nil {
			panic("Open sock " + err.Error())
		}
		_, err = conn.Write([]byte{1,2,3})
		if err != nil {
			panic("Write sock " + err.Error())
		}
		conn.Close()
	}
	for _, value := range listeners {
		defer (*value).Close()
		sock, err := (*value).Accept()
		if err != nil {
			panic("Accept sock " + err.Error())
		}
		defer sock.Close()
		data := make([]byte, 10)
		if _, err := sock.Read(data); err != nil {
			panic("Read sock " + err.Error())
		}
	}
	task.Wait()
	_, state = task.GetState()
	if state != "finished" {
		t.Error("Try to finish task faild")
	}
}

func TestStreamIntegrity(t *testing.T) {
	err := os.RemoveAll("/tmp/out.sock")
	if err != nil {
		panic("Remove socket /tmp/out.sock " + err.Error())
	}
	l, err := net.Listen("unix", "/tmp/out.sock")
	if err != nil {
		panic("Init sock " + err.Error())
	}
	defer l.Close()
	defer os.Remove("/tmp/out.sock")
	original_data := make([]byte, 500000000)
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
	message_size := 49152
	_, err = createStream("/tmp/in.sock", "/tmp/out.sock", ".", 1066, uint64(message_size), 0, 0)
	if err != nil {
		panic("Create stream " + err.Error())
	}
	wg.Wait()
	defer sock.Close()
	wg.Add(1)
	err = nil
	var recv_data []byte
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		next := true
		for next {
			data := make([]byte, message_size)
			count, err := sock.Read(data)
			if err != nil {
				if err == io.EOF {
					next = false
				} else {
					panic("Read error " + err.Error())
				}
			}
			recv_data = append(recv_data, data[:count]...)
		}
	}(&wg)
	c, err := net.Dial("unix", "/tmp/in.sock")
	if err != nil {
		panic("Open sock " + err.Error())
	}
	_, err = c.Write(original_data)
	if err != nil {
		panic("Write error " + err.Error())
	}
	c.Close()
	wg.Wait()
	if !bytes.Equal(original_data, recv_data) {
		t.Error("Integrity test fail original data is corrupted")
	}
}

