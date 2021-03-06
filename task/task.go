package task

import (
	"errors"
	"github.com/beeker1121/goque"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	s_done    = 1
	s_break   = 2
	s_fail    = 4
	s_perform = 8
	s_closed  = 16
)

type stream struct {
	parentId    uint64
	id          uint64
	messageSize uint64
	cache       *goque.Queue
	inSock      *net.UnixListener
	outSock     *net.UnixConn
	inSockPath  string
	outSockPath string
	state       uint32
	breakCh     chan struct{}
	doneCh      chan struct{}
	errorCh     chan error
	dataCh      chan *[]byte
	err         error
}

func (s *stream) done() {
	if s.state&s_done != s_done {
		s.doneCh <- struct{}{}
	}
}

func (s *stream) fail(err error) {
	if s.state&s_fail != s_fail {
		s.errorCh <- err
	}
}

func (s *stream) breakStream() {
	if s.state&s_break != s_break {
		s.breakCh <- struct{}{}
	}
}

func (s *stream) waitStream() {
	for s.state&s_closed != s_closed {
		time.Sleep(100 * time.Microsecond)
	}
}

func (s *stream) write(data *[]byte) {
	if s.cache.Length() == 0 {
		select {
		case s.dataCh <- data:
		default:
			_, err := s.cache.Enqueue(*data)
			if err != nil {
				s.fail(errors.New("Error while writing to cache: " + err.Error()))
				return
			}
		}
	} else {
		s.cache.Enqueue(*data)
		for len(s.dataCh) != cap(s.dataCh) && s.cache.Length() != 0 {
			item, err := s.cache.Dequeue()
			if err != nil {
				s.fail(errors.New("Error while reading data from cache: " + err.Error()))
				return
			}
			select {
			case s.dataCh <- &item.Value:
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
		flushed := false
		for !flushed {
			select {
			case s.dataCh <- &item.Value:
				flushed = true
			default:
				if s.state&s_break == s_break {
					return false
				}
				time.Sleep(5 * time.Millisecond)
			}
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
		if s.state&s_break == s_break {
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
		if s.state&s_break == s_break {
			return
		}
		count := 0
		data := make([]byte, s.messageSize)
		sock.SetReadDeadline(time.Now().Add(5 * time.Second))
		if count, err = sock.Read(data); err == io.EOF {
			if s.flush() {
				s.done()
				log.Println("Connection ", s.inSockPath, " is closed on the other side")
			}
			return
		} else if err, ok := err.(net.Error); ok && err.Timeout() {
			if count == 0 {
				continue
			}
		} else if err != nil {
			s.fail(errors.New("Error while reading data from " + s.inSockPath + ": " + err.Error()))
			return
		}
		if count != 0 {
			data = data[:count]
			s.write(&data)
		}
	}
}

func (s *stream) writer(wg *sync.WaitGroup) {
	defer wg.Done()
	defer s.outSock.Close()
	defer os.Remove(s.inSockPath + "." + strconv.FormatUint(s.parentId, 10) + "_" + strconv.FormatUint(s.id, 10))
	for {
		select {
		case data := <-s.dataCh:
			for {
				if s.state&s_break == s_break {
					return
				}
				s.outSock.SetWriteDeadline(time.Now().Add(5 * time.Second))
				count, err := s.outSock.Write(*data)
				if err, ok := err.(net.Error); ok && err.Timeout() {
					if count != 0 {
						(*data) = (*data)[count:]
					}
					continue
				} else if err != nil {
					s.fail(errors.New("Error while writing data to " + s.outSockPath + ": " + err.Error()))
					return
				}
				break
			}
		default:
			if s.state&s_break == s_break {
				return
			}
			if s.state&s_done == s_done {
				s.breakStream()
			}
			time.Sleep(5 * time.Millisecond)
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
		case <-s.doneCh:
			s.state = s.state &^ s_perform
			s.state |= s_done
		case err := <-s.errorCh:
			s.err = err
			s.state = s.state &^ s_perform
			s.state |= s_fail
			s.state |= s_break
			log.Println(err)
			break Controller
		case <-s.breakCh:
			s.state = s.state &^ s_perform
			s.state |= s_break
			break Controller
		}
	}
	wg.Wait()
	close(s.dataCh)
	close(s.errorCh)
	close(s.doneCh)
	close(s.breakCh)
	s.state |= s_closed
}

func createStream(inSocketPath string, outSocketPath string, cacheDir string, limitQueue, messageSize, pId, cId uint64) (*stream, error) {
	if limitQueue == 0 || messageSize == 0 {
		return nil, errors.New("Incorrect stream properties")
	}
	err := os.RemoveAll(cacheDir + "/" + strconv.FormatUint(pId, 10) + "_" + strconv.FormatUint(cId, 10))
	if err != nil {
		return nil, err
	}
	err = os.RemoveAll(inSocketPath + "." + strconv.FormatUint(pId, 10) + "_" + strconv.FormatUint(cId, 10))
	if err != nil {
		return nil, err
	}
	err = os.RemoveAll(inSocketPath)
	if err != nil {
		return nil, err
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
		parentId:    pId,
		id:          cId,
		messageSize: messageSize,
		cache:       pQ,
		inSock:      iS,
		outSock:     oS,
		inSockPath:  inSocketPath,
		outSockPath: outSocketPath,
		state:       0,
		dataCh:      make(chan *[]byte, limitQueue),
		breakCh:     make(chan struct{}, 10),
		doneCh:      make(chan struct{}, 10),
		errorCh:     make(chan error, 10),
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
	if s.state&s_done == s_done {
		return "finished"
	}
	if s.state&s_break == s_break {
		return "breaked"
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

func CreateTask(sockets [][]string, cacheDir string, id, limitQueue, messageSize uint64) (*Task, error) {
	sts := make([]*stream, len(sockets))
	var cleanup []int
	var err error
	for i, socks := range sockets {
		sts[i], err = createStream(socks[0], socks[1], cacheDir, limitQueue, messageSize, id, uint64(i))
		if err != nil {
			for _, index := range cleanup {
				sts[index].breakStream()
			}
			return nil, err
		}
		cleanup = append(cleanup, i)
	}
	return &Task{id: id, streams: sts}, nil
}

func (t *Task) Close() {
	for _, st := range t.streams {
		st.breakStream()
	}
	t.Wait()
}

func (t *Task) Wait() {
	for _, st := range t.streams {
		st.waitStream()
	}
}

type TaskState struct {
	Streams *[]map[string]string
	Task    string
}

func (t *Task) GetState() (*[]map[string]string, string) {
	var states []map[string]string
	failed := false
	breaked := false
	count := 0
	for _, st := range t.streams {
		state := make(map[string]string)
		state["state"] = st.GetState()
		if state["state"] == "breaked" {
			breaked = true
		}
		if state["state"] == "finished" {
			count++
		}
		if state["state"] == "failed" {
			failed = true
			state["error"] = st.GetError().Error()
		} else {
			state["error"] = "None"
		}
		state["id"] = strconv.FormatUint(st.GetId(), 10)
		state["inSocket"] = st.GetInputSocketPath()
		state["outSocket"] = st.GetOutputSocketPath()
		states = append(states, state)
	}
	if count == len(states) {
		return &states, "finished"
	}
	if failed {
		return &states, "failed"
	}
	if breaked {
		return &states, "breaked"
	}
	return &states, "performing"
}
