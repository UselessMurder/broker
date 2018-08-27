package main

import (
	"testing"
	"net"
	"os"
	"io/ioutil"
	"strconv"
	"github.com/cloudfoundry/gosigar"
	_"sync"
	"math/rand"
	"fmt"
)

func TestRouter(t *testing.T) {
	r := NewRouter("")
	defer r.Done()
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
	s := make([]string, 2)
	s[0] = "/tmp/in.sock"
	s[1] = "/tmp/out.sock"
	var ss [][]string
	ss = append(ss, s)
	id, err := r.CreateTask(&ss)
	if err != nil {
		t.Error(err.Error())
	}
	state, err := r.GetState(id)
	if err != nil {
		t.Error(err.Error())
	}
	if state.Task != "performing" {
		t.Error("Error missmatch task state")
	}
	if err := r.RemoveTask(id); err != nil {
		t.Error(err.Error())
	}
	if _, err := r.GetState(id); err == nil {
		t.Error(err.Error())
	}
}

var config_str = `CacheDir: /var/mem_broker_cache 
Queue_limit: 50 
Message_size: 500 
Max_mem: 0.5
`

func TestConfig(t *testing.T) {
	err := ioutil.WriteFile("./config", []byte(config_str), 0744)
	if err != nil {
		panic("Create config error " + err.Error())
	}
	defer os.RemoveAll("./config")
	r := NewRouter("./config")
	defer r.Done()
	if r.message_size != 500 || r.queue_limit != 50 {
		t.Error("Config is not appled")
	}
}


var config_str_mem = `CacheDir: /var/mem_broker_cache 
Queue_limit: 4096
Message_size: 5000
Max_mem: 0.5
`

func TestMemUsage(t *testing.T) {
	err := ioutil.WriteFile("./config", []byte(config_str_mem), 0744)
	if err != nil {
		panic("Create config error " + err.Error())
	}
	defer os.RemoveAll("./config")
	r := NewRouter("./config")
	defer r.Done()
	if r.message_size != 5000 || r.queue_limit != 4096 {
		t.Error("Config is not appled")
	}
	i := 0
	var ids []uint64
	var socks []string
	var listners []*net.Listener
	for {
		err := os.RemoveAll("/tmp/out.sock" + strconv.Itoa(i))
		if err != nil {
			panic("Remove socket /tmp/out.sock " + err.Error())
		}
		l, err := net.Listen("unix", "/tmp/out.sock" + strconv.Itoa(i))
		listners = append(listners, &l)
		socks = append(socks, "tmp/out.sock" + strconv.Itoa(i))
		if err != nil {
			panic("Init sock " + err.Error())
		}
		s := make([]string, 2)
		s[0] = "/tmp/in.sock" + strconv.Itoa(i)
		s[1] = "/tmp/out.sock"+ strconv.Itoa(i)
		var ss [][]string
		ss = append(ss, s)
		id , err := r.CreateTask(&ss)
		if err == nil {
			c, err := net.Dial("unix", "/tmp/in.sock" + strconv.Itoa(i))
			if err != nil {
				panic("Connect to sock error: " + err.Error())
			}
			for j := 0; j < int(r.queue_limit); j++ {
				buf := make([]byte, r.message_size)
				rand.Read(buf)
				_, err := c.Write(buf)
				if err != nil {
					fmt.Println(j)
					break
				}
			}
			c.Close()
			mem := sigar.Mem{}
			mem.Get()
			fmt.Println("Total: ", mem.Total, "Limit with 50%: ", uint64(float64(mem.Total) * r.max_mem), " ", "Used: ", mem.Used)
			ids = append(ids, id)
		} else if err.Error() == "Not enough memory to create task" {
			mem := sigar.Mem{}
			mem.Get()
			if uint64(float64(mem.Total) * 0.515) < mem.Used {
				t.Error("Incorrect memory usage!")
			}
			break
		} else {
			panic("Unknown error " + err.Error())
		}
	}
	for _, val := range ids {
		r.RemoveTask(val)
	}
	for _,val := range listners {
		(*val).Close()
	}
	for _, val := range socks {
		os.RemoveAll(val)
	}
}
