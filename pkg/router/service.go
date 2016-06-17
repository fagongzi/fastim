package router

import (
	"fmt"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/fagongzi/fastim/pkg/bind"
	"github.com/fagongzi/fastim/pkg/conf"
	"github.com/fagongzi/fastim/pkg/model"
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/fastim/pkg/registor"
	"github.com/fagongzi/fastim/pkg/util"
	"github.com/fagongzi/goetty"
	proto "github.com/golang/protobuf/proto"
	"strings"
	"sync"
	"time"
)

const (
	BIND_TIMEOUT_SECONDS = "4000"
)

type transportMap struct {
	sync.RWMutex
	key        string
	transports map[string]Transport
}

type Transport interface {
	Id() string
	GetProduct() int32
	Write(msg *p.Message) error
	StartWriteLoop()
	Close() error
}

type Service struct {
	cnf           *conf.RouterConf
	routing       bind.Routing
	transportsMap map[int]*transportMap

	registor registor.Registor
	backends *Backends

	watchStopCh chan bool
	stopping    bool

	shutdown *p.Message

	pool *sync.Pool

	routerHandlers map[int32]func(*p.Message) error
	crashRouters   chan *model.Router
}

// router service. goroutines about:
// start:
//     1. watch support,router from etcd
//     2. process crash other router
func NewService(cnf *conf.RouterConf, routing bind.Routing, registor registor.Registor) *Service {
	s := &Service{
		cnf:           cnf,
		routing:       routing,
		transportsMap: make(map[int]*transportMap, cnf.BucketSize),
		watchStopCh:   make(chan bool),

		registor: registor,
		shutdown: &p.Message{Cmd: proto.Int32(int32(p.RouterCMD_CHANGE_ROUTER))},

		pool: &sync.Pool{},

		routerHandlers: make(map[int32]func(*p.Message) error),
		crashRouters:   make(chan *model.Router, 1024),
	}

	s.backends = NewBackends(cnf, s)

	for i := 0; i < cnf.BucketSize; i++ {
		s.transportsMap[i] = &transportMap{
			transports: make(map[string]Transport),
		}
	}

	s.bindWatchHandlers()
	s.bindRouterCMDHandlers()

	go s.doWatch()

	return s
}

func (self *Service) stop() {
	self.stopping = true
	self.watchStopCh <- true
	self.broadcast(self.shutdown)

	// TODO: wait goroutines complete
}

func (self *Service) isStopping() bool {
	return self.stopping
}

func (self *Service) registerRouter(protocol model.RouterProtocol) error {
	router := &model.Router{
		Addr:         util.ConvertToIp(self.cnf.Addr),
		InternalAddr: util.ConvertToIp(self.cnf.InternalAddr),
		Protocol:     protocol,
		BucketSize:   self.cnf.BucketSize,
	}
	return self.registor.RegisterRouter(router, self.cnf.Etcd.RegisterTTL)
}

func (self *Service) deregisterRouter(protocol model.RouterProtocol) {
	router := &model.Router{
		Addr:         util.ConvertToIp(self.cnf.Addr),
		InternalAddr: util.ConvertToIp(self.cnf.InternalAddr),
		Protocol:     protocol,
		BucketSize:   self.cnf.BucketSize,
	}
	self.registor.DeregisterRouter(router)
}

func (self *Service) addTransport(id string, transport Transport) {
	tm := self.getTransportMap(id)
	tm.Lock()
	defer tm.Unlock()

	tm.transports[id] = transport

	self.doBind(id, transport, tm.key)
}

func (self *Service) doBind(id string, transport Transport, bucketKey string) {
	err := self.routing.Bind(id, BIND_TIMEOUT_SECONDS)
	if err != nil {
		log.ErrorErrorf(err, "%s id<%s> bind router failure", util.MODULE_SERVICE, id)
		return
	}

	if bucketKey != "" {
		self.routing.BindSession(bucketKey, id, transport.GetProduct())
	}

	log.Infof("%s id<%s> binded", util.MODULE_SERVICE, id)
	util.GetTimeWheel().AddWithId(time.Hour, fmt.Sprintf("%s-%s", id, "transport"), self.timeout)
}

func (self *Service) timeout(key string) {
	id := strings.Split(key, "-")[0]

	transport := self.getTransport(id)

	if transport != nil {
		self.doBind(id, transport, "")
	}
}

func (self *Service) deleteTransport(id string) {
	self.notifySessionClosed(id)
}

func (self *Service) getTransport(id string) Transport {
	tm := self.getTransportMap(id)
	tm.RLock()
	defer tm.RUnlock()

	return tm.transports[id]
}

func (self *Service) getTransportMap(id string) *transportMap {
	return self.transportsMap[goetty.HashCode(id)%self.cnf.BucketSize]
}

func (self *Service) broadcast(msg *p.Message) {
	for _, tm := range self.transportsMap {
		func() {
			tm.RLock()
			defer tm.RUnlock()

			for _, transport := range tm.transports {
				transport.Write(msg)
			}
		}()
	}
}

func (self *Service) doFromFrontend(msg *p.Message) error {
	cmd := msg.GetCmd()

	if _, ok := p.RouterCMD_name[cmd]; ok {
		handler, ok := self.routerHandlers[cmd]
		if ok {
			return handler(msg)
		}

		log.Warnf("%s id<%s> cmd<%d, %s> from frontend handler not found", util.MODULE_SERVICE, msg.GetSessionId(), cmd, p.RouterCMD_name[cmd])
	} else {
		addr, err := self.routing.GetBackend(msg)
		if err != nil {
			return err
		}

		transport := self.getTransport(msg.GetSessionId())
		if nil != transport {
			return self.backends.SendTo(addr, msg)
		} else {
			log.Warnf("%s id<%s> transport not found", util.MODULE_SERVICE, msg.GetSessionId())
		}
	}

	return nil
}

func (self *Service) doFromBackend(msg *p.Message) error {
	cmd := msg.GetCmd()

	if _, ok := p.RouterCMD_name[cmd]; ok {
		handler, ok := self.routerHandlers[cmd]
		if ok {
			return handler(msg)
		}

		log.Warnf("%s id<%s> cmd<%d, %s> from backend handler not found", util.MODULE_SERVICE, msg.GetSessionId(), cmd, p.RouterCMD_name[cmd])
	} else {
		// drop backend msg, when stopped
		if self.stopping {
			return nil
		}

		transport := self.getTransport(msg.GetSessionId())
		if nil != transport {
			return transport.Write(msg)
		} else {
			log.Warnf("%s id<%s> transport not found", util.MODULE_SERVICE, msg.GetSessionId())
		}
	}

	return nil
}

func (self *Service) notifySessionClosed(id string) {
	transport := self.getTransport(id)

	if nil == transport {
		log.Warnf("%s id<%s> transport not found", util.MODULE_SERVICE, id)
		return
	}

	bindSessionAddr := self.routing.GetSessionBackend(transport.GetProduct())
	if "" == bindSessionAddr {
		log.Warnf("%s id<%s> product<%d> session backend not found", util.MODULE_SERVICE, id, transport.GetProduct())
		return
	}

	msg, _ := self.pool.Get().(*p.Message)
	if nil == msg {
		msg = &p.Message{}
		msg.Cmd = proto.Int32(int32(p.RouterCMD_SESSION_CLOSED))
		msg.SessionId = proto.String(id)
	}

	defer self.pool.Put(msg)

	for i := 0; i < self.cnf.MaxRetry; i++ {
		err := self.backends.SendTo(bindSessionAddr, msg)
		if nil == err {
			log.Infof("%s id<%s> notify session closed to backend<%s>", util.MODULE_SERVICE, id, bindSessionAddr)
			util.GetTimeWheel().AddWithId(self.cnf.TimeoutWaitAck, getIdTimeoutKey(id), self.waitForSessionClosedAckTimeout)
			return
		}

		log.WarnErrorf(err, "%s id<%s> notify session closed to backend<%s> retry<%d> failure", util.MODULE_SERVICE, id, bindSessionAddr, i)

		bindSessionAddr = self.routing.GetSessionBackend(transport.GetProduct())
		if "" == bindSessionAddr {
			log.Warnf("%s id<%s> product<%d> session backend not found", util.MODULE_SERVICE, id, transport.GetProduct())
			return
		}
	}

	log.Warnf("%s id<%s> notify session closed to backend<%s> failure over max retry times<%d>", util.MODULE_SERVICE, id, bindSessionAddr, self.cnf.MaxRetry)
}

func (self *Service) waitForSessionClosedAckTimeout(key string) {
	self.notifySessionClosed(parseIdTimeoutKey(key))
}

func (self *Service) bindWatchHandlers() {
	self.registor.RegistorWatchHandler(registor.EVT_SRC_SUPPORT, registor.EVT_TYPE_NEW, self.addSupport)
	self.registor.RegistorWatchHandler(registor.EVT_SRC_SUPPORT, registor.EVT_TYPE_DELETE, self.deleteSupport)

	self.registor.RegistorWatchHandler(registor.EVT_SRC_ROUTER, registor.EVT_TYPE_NEW, self.addRouter)
	self.registor.RegistorWatchHandler(registor.EVT_SRC_ROUTER, registor.EVT_TYPE_DELETE, self.deleteRouter)
}

func (self *Service) doWatch() {
	err := self.registor.Watch(self.watchStopCh)
	if err != nil {
		log.WarnErrorf(err, "%s watch failed", util.MODULE_SERVICE)
	}
}

func (self *Service) addSupport(evt *registor.Evt) {
	support, _ := evt.Value.(*model.Support)
	self.routing.AddSupport(support)
}

func (self *Service) deleteSupport(evt *registor.Evt) {
	support, _ := evt.Value.(*model.Support)
	self.routing.DeleteSupport(support)
}

func (self *Service) addRouter(evt *registor.Evt) {
	router, _ := evt.Value.(*model.Router)
	self.routing.AddRouter(router)
}

func (self *Service) deleteRouter(evt *registor.Evt) {
	router, _ := evt.Value.(*model.Router)
	router = self.routing.DeleteRouter(router)

	if nil != router {
		self.crashRouters <- router
	}
}

func (self *Service) doRouterCrash() {
	for {
		router := <-self.crashRouters
		fmt.Println(router)
	}
}

func getIdTimeoutKey(id string) string {
	return fmt.Sprintf("ack:%s", id)
}

func parseIdTimeoutKey(key string) string {
	return strings.Split(key, ":")[1]
}
