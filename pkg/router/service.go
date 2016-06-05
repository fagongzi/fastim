package router

import (
	"fmt"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/fagongzi/fastim/pkg/bind"
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/fastim/pkg/util"
	"github.com/fagongzi/goetty"
	proto "github.com/golang/protobuf/proto"
	"strings"
	"sync"
	"time"
)

const (
	BIND_TIMEOUT_SECONDS = "3600"
)

type transportMap struct {
	sync.RWMutex
	transports map[string]Transport
}

type Transport interface {
	WriteToFrontend(msg *p.Message) error
	WriteToBackend(addr string, msg *p.Message) error
}

type Service struct {
	sync.RWMutex
	cnf           *Conf
	routing       bind.Routing
	transportsMap map[int]*transportMap
}

func NewService(cnf *Conf, routing bind.Routing) *Service {
	s := &Service{
		cnf:           cnf,
		routing:       routing,
		transportsMap: make(map[int]*transportMap, BUCKET_TRANSPORT_SIZE),
	}

	for i := 0; i < BUCKET_TRANSPORT_SIZE; i++ {
		s.transportsMap[i] = &transportMap{
			transports: make(map[string]Transport),
		}
	}

	return s
}

func (self *Service) addTransport(id string, transport Transport) {
	tm := self.transportsMap[goetty.HashCode(id)%BUCKET_TRANSPORT_SIZE]
	tm.Lock()
	defer tm.Unlock()

	tm.transports[id] = transport

	self.doBind(id)
}

func (self *Service) doBind(id string) {
	self.routing.Bind(id, BIND_TIMEOUT_SECONDS)
	log.Infof("%s id<%s> bind", util.MODULE_SERVICE, id)
	util.GetTimeWheel().AddWithId(time.Hour, fmt.Sprintf("%s-%s", id, "transport"), self.timeout)
}

func (self *Service) timeout(key string) {
	id := strings.Split(key, "-")[0]

	tm := self.transportsMap[goetty.HashCode(id)%BUCKET_TRANSPORT_SIZE]
	tm.RLock()
	defer tm.RUnlock()

	_, ok := tm.transports[id]
	if ok {
		self.doBind(id)
	}
}

func (self *Service) deleteTransport(id string) {
	tm := self.transportsMap[goetty.HashCode(id)%BUCKET_TRANSPORT_SIZE]
	tm.Lock()
	defer tm.Unlock()

	delete(tm.transports, id)
	self.routing.Unbind(id)
	log.Infof("%s id<%s> unbind", util.MODULE_SERVICE, id)

	// TODO: send to backend, confirm least one backend received message
	closedNotify := &p.Message{}
	closedNotify.Cmd = proto.Int32(p.RouterCMD_SESSION_CLOSED)
	closedNotify.SessionId = proto.String(id)
}

func (self *Service) getTransport(id string) Transport {
	tm := self.transportsMap[goetty.HashCode(id)%BUCKET_TRANSPORT_SIZE]
	tm.RLock()
	defer tm.RUnlock()

	return tm.transports[id]
}

func (self *Service) doFromFrontend(msg *p.Message) error {
	if _, ok := p.RouterCMD_name[msg.GetCmd()]; ok {
		//TODO: 处理router命令
	} else {
		addr, err := self.routing.SelectBackend(msg)
		if err != nil {
			return err
		}

		transport := self.getTransport(msg.GetSessionId())
		if nil != transport {
			return transport.WriteToBackend(addr, msg)
		} else {
			log.Warnf("%s id<%s> transport not found", util.MODULE_SERVICE, msg.GetSessionId())
		}
	}

	return nil
}

func (self *Service) doFromBackend(msg *p.Message) error {
	if _, ok := p.RouterCMD_name[msg.GetCmd()]; ok {
		//TODO: 处理router命令
	} else {
		transport := self.getTransport(msg.GetSessionId())
		if nil != transport {
			return transport.WriteToFrontend(msg)
		} else {
			log.Warnf("%s id<%s> transport not found", util.MODULE_SERVICE, msg.GetSessionId())
		}
	}

	return nil
}
