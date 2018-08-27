package main

import (
	"encoding/json"
	"fmt"
	"github.com/cloudfoundry/gosigar"
	"github.com/go-resty/resty"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"strconv"
	"testing"
)

func TestRouter(t *testing.T) {
	r := NewRouter("")
	defer r.Done()
	err := os.RemoveAll("/tmp/out.sock")
	if err != nil {
		panic("Error while trying remove socket /tmp/out.sock: " + err.Error())
	}
	l, err := net.Listen("unix", "/tmp/out.sock")
	if err != nil {
		panic("Error while trying listen socket /tmp/out.sock " + err.Error())
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

var config_str = `cache_dir: /var/mem_broker_cache 
queue_limit: 50 
message_size: 500 
max_mem: 0.5
service_port: 8098
`

func TestConfig(t *testing.T) {
	err := ioutil.WriteFile("./config", []byte(config_str), 0744)
	if err != nil {
		panic("Error while trying create config file ./config: " + err.Error())
	}
	defer os.RemoveAll("./config")
	r := NewRouter("./config")
	defer r.Done()
	if r.messageSize != 500 || r.queueLimit != 50 {
		t.Error("Config is not appled")
	}
}

func TestREST(t *testing.T) {
	r := NewRouter("")
	defer r.Done()
	go r.Service()
	err := os.RemoveAll("/tmp/out.sock")
	if err != nil {
		panic("Error while trying remove socket /tmp/out.sock: " + err.Error())
	}
	l, err := net.Listen("unix", "/tmp/out.sock")
	if err != nil {
		panic("Error while trying listen socket /tmp/out.sock " + err.Error())
	}
	defer l.Close()
	defer os.Remove("/tmp/out.sock")
	s := make([]string, 2)
	s[0] = "/tmp/in.sock"
	s[1] = "/tmp/out.sock"
	var ss [][]string
	ss = append(ss, s)
	resp, err := resty.R().
		SetHeader("Content-Type", "application/json").
		SetBody(CreateStructIn{Sockets: ss}).
		Post("http://localhost:8098/task")
	if err != nil {
		panic("Error while trying perform create request: " + err.Error())
	}
	cres := CreateStructOut{}
	err = json.Unmarshal(resp.Body(), &cres)
	if err != nil {
		panic("Error while trying unmarshal create response: " + err.Error())
	}
	if len(cres.Error) > 0 {
		t.Error("Error while trying to create task: ", cres.Error)
	}
	resp, err = resty.R().
		SetQueryParams(map[string]string{
			"id": strconv.FormatUint(cres.Id, 10),
		}).
		SetHeader("Accept", "application/json").
		Get("http://localhost:8098/task")
	if err != nil {
		panic("Error while trying perform state request: " + err.Error())
	}
	state := StateStruct{}
	err = json.Unmarshal(resp.Body(), &state)
	if err != nil {
		panic("Error while trying unmarshal get state response: " + err.Error())
	}
	if len(state.Error) > 0 || state.TaskState != "performing" {
		t.Error("Error while trying get state of task: ", state.Error)
	}
	resp, err = resty.R().
		SetQueryParams(map[string]string{
			"id": strconv.FormatUint(cres.Id, 10),
		}).SetHeader("Accept", "application/json").
		Delete("http://localhost:8098/task")
	if err != nil {
		panic("Error while trying perform remove request: " + err.Error())
	}
	result := DefaultResult{}
	err = json.Unmarshal(resp.Body(), &result)
	if err != nil {
		panic("Error while trying unmarshal remove response: " + err.Error())
	}
	if len(result.Error) > 0 {
		t.Error("Error while trying remove task: ", state.Error)
	}
	resp, err = resty.R().
		SetQueryParams(map[string]string{
			"id": strconv.FormatUint(cres.Id, 10),
		}).
		SetHeader("Accept", "application/json").
		Get("http://localhost:8098/task")
	if err != nil {
		panic("Error while trying perform state request: " + err.Error())
	}
	state = StateStruct{}
	err = json.Unmarshal(resp.Body(), &state)
	if err != nil {
		panic("Error while trying unmarshal get state response: " + err.Error())
	}
	if len(state.Error) < 0 {
		t.Error("Error while trying get state of task: ", state.Error)
	}
	//resp, err = resty.R().
	//SetHeader("Accept", "application/json").
	//Delete("http://localhost:8098/self")
	//if err != nil {
	//panic("Error while trying perform remove request: " + err.Error())
	//}

}

var config_str_mem = `cache_dir: /var/mem_broker_cache 
queue_limit: 1066
message_size: 49152
max_mem: 0.90
service_port: 8098
`

func TestMemUsage(t *testing.T) {
	err := ioutil.WriteFile("./config", []byte(config_str_mem), 0744)
	if err != nil {
		panic("Error while trying create config file ./config: " + err.Error())
	}
	defer os.RemoveAll("./config")
	r := NewRouter("./config")
	defer r.Done()
	if r.messageSize != 49152 || r.queueLimit != 1066 {
		t.Error("Config is not appled")
	}
	i := 0
	var ids []uint64
	var socks []string
	var listners []*net.Listener
	for {
		err := os.RemoveAll("/tmp/out.sock" + strconv.Itoa(i))
		if err != nil {
			panic("Error while trying remove socket /tmp/out.sock" + strconv.Itoa(i) + ": " + err.Error())
		}
		l, err := net.Listen("unix", "/tmp/out.sock"+strconv.Itoa(i))
		if err != nil {
			panic("Error while trying listen socket /tmp/out.sock" + strconv.Itoa(i) + ": " + err.Error())
		}
		listners = append(listners, &l)
		socks = append(socks, "tmp/out.sock"+strconv.Itoa(i))
		s := make([]string, 2)
		s[0] = "/tmp/in.sock" + strconv.Itoa(i)
		s[1] = "/tmp/out.sock" + strconv.Itoa(i)
		var ss [][]string
		ss = append(ss, s)
		id, err := r.CreateTask(&ss)
		if err == nil {
			c, err := net.Dial("unix", "/tmp/in.sock"+strconv.Itoa(i))
			if err != nil {
				panic("Error while trying connect to socket /tmp/in.sock" + strconv.Itoa(i) + ": " + err.Error())
			}
			for j := 0; j < int(r.queueLimit); j++ {
				buf := make([]byte, r.messageSize)
				rand.Read(buf)
				_, err := c.Write(buf)
				if err != nil {
					break
				}
			}
			c.Close()
			mem := sigar.Mem{}
			mem.Get()
			fmt.Println("Total: ", mem.Total, "Limit with 90%: ", uint64(float64(mem.Total)*r.maxMem), " ", "Used: ", mem.Used)
			ids = append(ids, id)
		} else if err.Error() == "Not enough memory to create task" {
			mem := sigar.Mem{}
			mem.Get()
			if uint64(float64(mem.Total)*0.915) < mem.Used {
				t.Error("Incorrect memory usage!")
			}
			break
		} else {
			panic("Unknown error: " + err.Error())
		}
		i++
	}
	for _, val := range ids {
		r.RemoveTask(val)
	}
	for _, val := range listners {
		(*val).Close()
	}
	for _, val := range socks {
		os.RemoveAll(val)
	}
}
