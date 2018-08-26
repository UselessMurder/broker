package task

import (
	"errors"
	"github.com/beeker1121/goque"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
	"strconv"
	"fmt"
)

const (
	s_quit    = 1
	s_fail    = 2
	s_perform = 4
)

type stream struct {
	parent_id   uint64
	id          uint64
	cache       *goque.Queue
	inSock      *net.UnixListener
	outSock     *net.UnixConn
	inSockPath  string
	outSockPath string
	state       uint32
	done        chan struct{}
	errstream   chan error
	datastream  chan *[]byte
	err         error
}

func (s *stream) quit() {
	if s.state&s_quit != s_quit {
		s.done <- struct{}{}
	}
}

func (s *stream) fail(err error) {
	if s.state&s_fail != s_fail {
		s.errstream <- err
	}
}

func (s *stream) write(data *[]byte) {
	if s.cache.Length() == 0 {
		select {
		case s.datastream <- data:
		default:
			size, err := s.cache.Enqueue(*data)
			fmt.Println(len(*data), len(size.Value))
			if err != nil {
				s.fail(errors.New("Error while writing to cache: " + err.Error()))
				return
			}
		}
	} else {
		s.cache.Enqueue(*data)
		for len(s.datastream) != cap(s.datastream) && s.cache.Length() != 0 {
			item, err := s.cache.Dequeue()
			if err != nil {
				s.fail(errors.New("Error while reading data from cache: " + err.Error()))
				return
			}
			select {
			case s.datastream <- &item.Value:
			default:
				s.fail(errors.New("Error more one writets per stream"))
				return
			}
		}
	}
}

func (s *stream) flush() bool {
	for s.cache.Length() != 0 {
		item, err := s.cache.Dequeue()
		if err != nil {
			s.fail(errors.New("Error while reading data from cache: " + err.Error()))
			return false
		}
		select {
		case s.datastream <- &item.Value:
		default:
			if s.state&s_fail == s_fail {
				return false
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return true
}

func (s *stream) reader(wg *sync.WaitGroup) {
	defer wg.Done()
	defer s.inSock.Close()
	defer os.Remove(s.inSockPath)
	var sock *net.UnixConn
	var err error
	for {
		if s.state&s_quit == s_quit {
			return
		}
		s.inSock.SetDeadline(time.Now().Add(5 * time.Second))
		sock, err = s.inSock.AcceptUnix()
		if err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				continue
			} else {
				s.fail(errors.New("Error while waiting for connection: " + err.Error()))
				return
			}
		}
		break
	}
	defer sock.Close()
	for {
		if s.state&s_quit == s_quit {
			s.flush()
			return
		}
		count := 0
		data := make([]byte, 1024)
		sock.SetReadDeadline(time.Now().Add(5 * time.Second))
		if count, err = sock.Read(data); err == io.EOF {
			if s.flush() {
				s.quit()
				log.Println("Connection ", s.inSockPath, " closed")
			}
			return
		} else if err, ok := err.(net.Error); ok && err.Timeout() {
			continue
		} else if err != nil {
			s.fail(errors.New("Error while reading data from " + s.inSockPath + ": " + err.Error()))
			return
		}
		if (count != 0) {
			data = data[:count]
			s.write(&data)
		}
	}
}

func (s *stream) writer(wg *sync.WaitGroup) {
	defer wg.Done()
	defer s.outSock.Close()
	defer os.Remove(s.inSockPath + "." + strconv.FormatUint(s.parent_id, 10) + "_" + strconv.FormatUint(s.id, 10))
	for {
		select {
		case data := <-s.datastream:
			for {
				if s.state & s_fail == s_fail {
					return
				}
				s.outSock.SetWriteDeadline(time.Now().Add(5 * time.Second))
				_, err := s.outSock.Write(*data)
				if err, ok := err.(net.Error); ok && err.Timeout() {
					continue
				} else if err != nil {
					s.fail(errors.New("Error while writing data to " + s.outSockPath + ": " + err.Error()))
					return
				}
				break
			}
		default:
			if s.state & s_quit == s_quit {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (s *stream) controller() {
	var wg sync.WaitGroup
	defer s.cache.Drop()
	wg.Add(2)
	go s.writer(&wg)
	go s.reader(&wg)
	s.state |= s_perform
Controller:
	for {
		select {
		case <-s.done:
			s.state = s.state &^ s_perform
			s.state |= s_quit
			break Controller
		case err := <-s.errstream:
			s.err = err
			s.state = s.state &^ s_perform
			s.state |= s_fail
			s.state |= s_quit
			log.Println(err)
			break Controller
		}
	}
	wg.Wait()
	close(s.datastream)
	close(s.errstream)
	close(s.done)
}

func createStream(inSocketPath string, outSocketPath string, cacheDir string, limit, pId, cId uint64) (*stream, error) {
	if limit == 0 {
		return nil, errors.New("Can`t init stream with empty queue")
	}
	pQ, err := goque.OpenQueue(cacheDir + "/" + strconv.FormatUint(pId, 10) + "_" + strconv.FormatUint(cId, 10))
	if err != nil {
		return nil, err
	}
	iS, err := net.ListenUnix("unix", &net.UnixAddr{inSocketPath, "unix"})
	if err != nil {
		return nil, err
	}
	laddr := net.UnixAddr{inSocketPath + "." + strconv.FormatUint(pId, 10) + "_" + strconv.FormatUint(cId, 10), "unix"}
	oS, err := net.DialUnix("unix", &laddr, &net.UnixAddr{outSocketPath, "unix"})
	if err != nil {
		return nil, err
	}
	s := &stream{
		parent_id:   pId,
		id:          cId,
		cache:       pQ,
		inSock:      iS,
		outSock:     oS,
		inSockPath:  inSocketPath,
		outSockPath: outSocketPath,
		state:       0,
		datastream:  make(chan *[]byte, limit),
		done:        make(chan struct{}, 10),
		errstream:   make(chan error, 10),
		err:         nil,
	}
	go s.controller()
	return s, nil
}

func (s *stream) GetState() string {
	if s.state == 0 {
		return "dumy"
	}
	if s.state&s_fail == s_fail {
		return "failed"
	}
	if s.state&s_quit == s_quit {
		return "finished"
	}
	if s.state&s_perform == s_perform {
		return "performing"
	}
	return "dumy"
}

func (s *stream) GetId() uint64 {
	return s.id
}

func (s *stream) GetInputSocketPath() string {
	return s.inSockPath
}

func (s *stream) GetOutputSocketPath() string {
	return s.outSockPath
}

func (s *stream) GetError() error {
	return s.err
}

type Task struct {
	id      uint64
	streams []*stream
}

func CreateTask(sockets [][]string, cacheDir string, id, limit uint64) (*Task, error) {
	sts := make([]*stream, len(sockets))
	var cleanup []int
	var err error
	for i, socks := range sockets {
		sts[i], err = createStream(socks[0], socks[1], cacheDir, limit, id, uint64(i))
		if err != nil {
			for _, index := range cleanup {
				sts[index].quit()
			}
			return nil, err
		}
		cleanup = append(cleanup, i)
	}
	return &Task{id: id, streams: sts}, nil
}

func (t *Task) Close() {
	for _, st := range t.streams {
		st.quit()
	}
}

func (t *Task) GetState() ([]*map[string]string, string) {
	var states []*map[string]string
	failed := false
	count := 0
	for _, st := range t.streams {
		state := make(map[string]string)
		state["state"] = st.GetState()
		if state["state"] == "failed" {
			failed = true
		}
		if state["state"] == "finished" {
			count++
		}
		if state["state"] == "failed" {
			state["error"] = st.GetError().Error()
		} else {
			state["error"] = "None"
		}
		state["id"] = strconv.FormatUint(st.GetId(), 10)
		state["inSocket"] = st.GetInputSocketPath()
		state["outSocket"] = st.GetOutputSocketPath()
		states = append(states, &state)
	}
	if count == len(states) {
		return states, "finished"
	}
	if failed {
		return states, "failed"
	}
	return states, "performing"
}
