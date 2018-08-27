package main

import (
	"./task"
	"log"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"github.com/cloudfoundry/gosigar"
	"errors"
)



type feedback_msg struct {
	payload interface{}
	feed *chan interface{}
}

type TaskRouter struct {
	lastId uint64
	cacheDir string
	queue_limit uint64
	message_size uint64
	max_mem float64
	tasks map[uint64] *task.Task
	doneCh chan *feedback_msg
	removeCh chan *feedback_msg
	createCh chan *feedback_msg
	stateCh chan *feedback_msg
}

type Config struct {
	CacheDir string `yaml:"CacheDir"`
	Queue_limit uint64 `yaml:"Queue_limit"`
	Message_size uint64 `yaml:"Message_size"`
	Max_mem float64 `yaml:"Max_mem"`
}

func (c *Config) readConf(path string) {
	yamlFile, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalf("Config file read error #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
}

func (tr *TaskRouter) runRouter() {
	for {
		select {
		case msg := <-tr.doneCh:
			for _, value := range tr.tasks {
				value.Close()
			}
			*msg.feed <- struct{}{}
		case msg := <-tr.createCh:
			mem := sigar.Mem{}
			mem.Get()
			if uint64(float64(mem.Total) * tr.max_mem) < mem.Used {
				log.Println("Not enough memory to create task")
				*msg.feed <- errors.New("Not enough memory to create task")
				continue
			}
			ql := (uint64(float64(mem.Total) * tr.max_mem) - mem.Used) / (tr.message_size * uint64(len(*msg.payload.(*[][]string))))
			if ql == 0 {
				log.Println("Not enough memory to create task")
				*msg.feed <- errors.New("Not enough memory to create task")
				continue
			}
			if ql > tr.queue_limit {
				ql = tr.queue_limit
			}
			var err error
			tr.tasks[tr.lastId], err = task.CreateTask(*msg.payload.(*[][]string), tr.cacheDir, tr.lastId, ql,  tr.message_size)
			if err != nil {
				log.Println("Error when trying to create task ", err)
				*msg.feed <- err
			} else {
				*msg.feed <-tr.lastId
				tr.lastId++
			}
		case msg := <-tr.removeCh:
			if val, ok := tr.tasks[msg.payload.(uint64)]; ok {
				val.Close()
				delete(tr.tasks, msg.payload.(uint64))
				*msg.feed <- struct{}{}
			} else {
				*msg.feed <- errors.New("Task not exists")
			}
		case msg := <-tr.stateCh:
			if val, ok := tr.tasks[msg.payload.(uint64)]; ok {
				var state task.TaskState
				state.Streams, state.Task = val.GetState()
				*msg.feed <- &state
			} else {
				*msg.feed <- errors.New("Task not exists")
			}
		}
	}
}

func (tr *TaskRouter) CreateTask(sockets *[][]string) (uint64, error) {
	ch := make(chan interface{})
	tr.createCh <- &feedback_msg{payload: sockets, feed: &ch}
	ans := <-ch
	close(ch)
	if val, ok := ans.(error); ok {
		return 0, val
	}
	if val, ok := ans.(uint64); ok {
		return val, nil
	}
	log.Fatalln("Incorrect create answer!")
	return 0, nil
}

func (tr *TaskRouter) RemoveTask(id uint64) (error) {
	ch := make(chan interface{})
	tr.removeCh <- &feedback_msg{payload: id, feed: &ch}
	ans := <-ch
	close(ch)
	if val, ok := ans.(error); ok {
		return val
	}
	return nil
}

func (tr *TaskRouter) GetState(id uint64) (*task.TaskState, error) {
	ch := make(chan interface{})
	tr.stateCh <- &feedback_msg{payload: id, feed: &ch}
	ans := <-ch
	close(ch)
	if val, ok := ans.(error); ok {
		return nil, val
	}
	if val, ok := ans.(*task.TaskState); ok {
		return val, nil
	}
	log.Fatalln("Incorrect state answer!")
	return nil, nil
}

func (tr *TaskRouter) Done() {
	ch := make(chan interface{})
	tr.doneCh <- &feedback_msg{payload: nil, feed: &ch}
	<-ch
	close(ch)
}

func NewRouter(configPath string) *TaskRouter {
	var conf Config
	if len(configPath) <= 0 {
		log.Println("Init router with default parameters")
		conf.CacheDir = "/var/mem_broker_cache"
		conf.Queue_limit = 1066
		conf.Message_size = 49152
		conf.Max_mem = 0.90
	} else {
		log.Println("Init router from config file " + configPath)
		conf.readConf(configPath)
	}
	if _, err := os.Stat(conf.CacheDir); os.IsNotExist(err) {
		if err = os.MkdirAll(conf.CacheDir, os.ModePerm); err != nil {
			log.Fatalln(err, " ", conf.CacheDir)
		}
	}
	if conf.Max_mem == 0 || conf.Message_size == 0 || conf.Queue_limit == 0 {
		log.Fatalln("Incorrect configuration parameters")
	}
	t := &TaskRouter{
		lastId: 0,
		cacheDir: conf.CacheDir,
		queue_limit: conf.Queue_limit,
		message_size: conf.Message_size,
		max_mem: conf.Max_mem,
		tasks: make(map[uint64]*task.Task),
		doneCh: make(chan *feedback_msg, 10),
		createCh: make(chan *feedback_msg, 10),
		removeCh: make(chan *feedback_msg, 10),
		stateCh: make(chan *feedback_msg, 10),
	}
	go t.runRouter()
	return t
}

var router *TaskRouter

func main() {
	router = NewRouter(os.Args[1])
}
