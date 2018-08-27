package main

import (
	"./task"
	"errors"
	"github.com/cloudfoundry/gosigar"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
)

type feedback_msg struct {
	payload interface{}
	feed    *chan interface{}
}

type TaskRouter struct {
	lastId      uint64
	cacheDir    string
	queueLimit  uint64
	messageSize uint64
	maxMem      float64
	servicePort int
	tasks       map[uint64]*task.Task
	doneCh      chan *feedback_msg
	removeCh    chan *feedback_msg
	createCh    chan *feedback_msg
	stateCh     chan *feedback_msg
}

type Config struct {
	CacheDir    string  `yaml:"cache_dir"`
	QueueLimit  uint64  `yaml:"queue_limit"`
	MessageSize uint64  `yaml:"message_size"`
	MaxMem      float64 `yaml:"max_mem"`
	ServicePort int     `yaml:"service_port"`
}

func (c *Config) readConf(path string) {
	yamlFile, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalf("Config file read error: #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Fatalf("Yaml unmarshal error: %v", err)
	}
}

func (tr *TaskRouter) runRouter() {
	for {
		select {
		case msg := <-tr.doneCh:
			for _, value := range tr.tasks {
				value.Close()
			}
			log.Println("Task router exited")
			*msg.feed <- struct{}{}
		case msg := <-tr.createCh:
			mem := sigar.Mem{}
			mem.Get()
			if uint64(float64(mem.Total)*tr.maxMem) < mem.Used {
				log.Println("Not enough memory to create task:\n*** Total memory - ", mem.Total, "\n*** Used memory - ", mem.Used,
					"\n*** Allowed percent - ", tr.maxMem)
				*msg.feed <- errors.New("Not enough memory to create task")
				continue
			}
			ql := (uint64(float64(mem.Total)*tr.maxMem) - mem.Used) / (tr.messageSize * uint64(len(*msg.payload.(*[][]string))))
			if ql == 0 {
				log.Println("Not enough memory to create task:\n*** Total memory - ", mem.Total, "\n*** Used memory - ",
					mem.Used, "\n*** Allowed percent - ", tr.maxMem)
				*msg.feed <- errors.New("Not enough memory to create task")
				continue
			}
			if ql > tr.queueLimit {
				ql = tr.queueLimit
			}
			var err error
			tr.tasks[tr.lastId], err = task.CreateTask(*msg.payload.(*[][]string), tr.cacheDir, tr.lastId, ql, tr.messageSize)
			if err != nil {
				log.Println("Error when trying to create task ", err)
				*msg.feed <- err
			} else {
				log.Println("Task with id: ", tr.lastId, " created with params:\n*** queue length - ", ql, "\n*** message size - ", tr.messageSize,
					"\n*** streams - ", *msg.payload.(*[][]string))
				*msg.feed <- tr.lastId
				tr.lastId++
			}
		case msg := <-tr.removeCh:
			if val, ok := tr.tasks[msg.payload.(uint64)]; ok {
				val.Close()
				delete(tr.tasks, msg.payload.(uint64))
				log.Println("Task with id: ", msg.payload.(uint64), " was removed")
				*msg.feed <- struct{}{}
			} else {
				log.Println("Trying remove task with non-existen id:  ", msg.payload.(uint64))
				*msg.feed <- errors.New("Task not exists")
			}
		case msg := <-tr.stateCh:
			if val, ok := tr.tasks[msg.payload.(uint64)]; ok {
				var state task.TaskState
				state.Streams, state.Task = val.GetState()
				log.Println("Task with id: ", msg.payload.(uint64), " was sent, state: ", state)
				*msg.feed <- &state
			} else {
				log.Println("Trying get state of task with non-existen id:  ", msg.payload.(uint64))
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

func (tr *TaskRouter) RemoveTask(id uint64) error {
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

func (tr *TaskRouter) GetServicePort() int {
	return tr.servicePort
}

func (tr *TaskRouter) GetLogDir() string {
	return tr.cacheDir
}

func (tr *TaskRouter) Service() {
	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.POST("/task", func(c echo.Context) error {
		CSI := CreateStructIn{}
		if err := c.Bind(&CSI); err != nil {
			return c.JSON(http.StatusUnsupportedMediaType, CreateStructOut{Id: 0, Error: err.Error()})
		}
		id, err := tr.CreateTask(&CSI.Sockets)
		if err != nil {
			return c.JSON(http.StatusConflict, CreateStructOut{Id: id, Error: err.Error()})
		}
		return c.JSON(http.StatusCreated, CreateStructOut{Id: id})
	})
	e.DELETE("/task", func(c echo.Context) error {
		id, err := strconv.Atoi(c.QueryParam("id"))
		if err != nil {
			return c.JSON(http.StatusBadRequest, DefaultResult{Error: err.Error()})
		}
		err = tr.RemoveTask(uint64(id))
		if err != nil {
			return c.JSON(http.StatusNotFound, DefaultResult{Error: err.Error()})
		}
		return c.JSON(http.StatusOK, DefaultResult{Error: ""})
	})
	e.GET("/task", func(c echo.Context) error {
		id, err := strconv.Atoi(c.QueryParam("id"))
		if err != nil {
			return c.JSON(http.StatusBadRequest, StateStruct{Error: err.Error()})
		}
		state, err := tr.GetState(uint64(id))
		if err != nil {
			return c.JSON(http.StatusNotFound, StateStruct{Error: err.Error()})
		}
		return c.JSON(http.StatusOK, StateStruct{TaskState: state.Task, StreamStates: *state.Streams})
	})
	e.DELETE("/self", func(c echo.Context) error {
		tr.Done()
		defer os.Exit(0)
		return c.JSON(http.StatusOK, "Exited")
	})
	e.Start(":" + strconv.Itoa(tr.GetServicePort()))
}

func NewRouter(configPath string) *TaskRouter {
	var conf Config
	if len(configPath) <= 0 {
		log.Println("Init router with default parameters")
		conf.CacheDir = "/var/mem_broker_cache"
		conf.QueueLimit = 1066
		conf.MessageSize = 49152
		conf.MaxMem = 0.90
		conf.ServicePort = 8098
	} else {
		log.Println("Init router from config file " + configPath)
		conf.readConf(configPath)
	}
	if _, err := os.Stat(conf.CacheDir); os.IsNotExist(err) {
		if err = os.MkdirAll(conf.CacheDir, os.ModePerm); err != nil {
			log.Fatalln("Can`t create directory for cache ", conf.CacheDir, ": ", err)
		}
	}
	if conf.MaxMem == 0 || conf.MessageSize == 0 || conf.QueueLimit == 0 {
		log.Fatalln("Incorrect configuration parameters")
	}
	log.Println("Router parameters:\n*** cache\\log dir - ", conf.CacheDir, "\n*** service port - ", conf.ServicePort, "\n*** memory usage percent - ", conf.MaxMem,
		"\n*** queue limit - ", conf.QueueLimit, "\n*** message size - ", conf.MessageSize)
	tr := &TaskRouter{
		lastId:      0,
		cacheDir:    conf.CacheDir,
		queueLimit:  conf.QueueLimit,
		messageSize: conf.MessageSize,
		maxMem:      conf.MaxMem,
		servicePort: conf.ServicePort,
		tasks:       make(map[uint64]*task.Task),
		doneCh:      make(chan *feedback_msg, 10),
		createCh:    make(chan *feedback_msg, 10),
		removeCh:    make(chan *feedback_msg, 10),
		stateCh:     make(chan *feedback_msg, 10),
	}
	go tr.runRouter()
	return tr
}

type CreateStructIn struct {
	Sockets [][]string `json:"Sockets"`
}

type CreateStructOut struct {
	Id    uint64 `json:"Sockets"`
	Error string `json:"Error"`
}

type StateStruct struct {
	TaskState    string              `json:"TaskState"`
	StreamStates []map[string]string `json:"StreamStates"`
	Error        string              `json:"Error"`
}

type DefaultResult struct {
	Error string `json:"Error"`
}

func main() {
	var router *TaskRouter
	if len(os.Args) > 1 {
		router = NewRouter(os.Args[1])
	} else {
		router = NewRouter("")
	}
	defer router.Done()
	f, err := os.OpenFile(router.GetLogDir()+"/broker.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Error opening log file ", router.GetLogDir(), "/broker.log:", err)
	}
	log.SetOutput(f)
	router.Service()
}
