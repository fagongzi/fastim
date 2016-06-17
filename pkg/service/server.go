package service

import (
	"github.com/CodisLabs/codis/pkg/utils/atomic2"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/fagongzi/fastim/pkg/bind"
	"github.com/fagongzi/fastim/pkg/conf"
	c "github.com/fagongzi/fastim/pkg/connection"
	"github.com/fagongzi/fastim/pkg/model"
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/fastim/pkg/registor"
	"github.com/fagongzi/fastim/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/golang/protobuf/proto"
)

var counter int64
var tokenGen atomic2.Int64

type CMDHandler interface {
	Handler(msg *p.Message) error
}

type Server struct {
	cnf     *conf.ServiceConf
	support *model.Support

	tcpServer *goetty.Server

	tw   *goetty.HashedTimeWheel
	pool *c.ConnectorPool

	registor     registor.Registor
	routing      bind.Routing
	syncProtocol p.SyncProtocol

	handlers map[int32]CMDHandler

	queue chan *p.Message

	batch map[int64]map[string]int64
}

func NewServer(cnf *conf.ServiceConf, support *model.Support, routing bind.Routing, registor registor.Registor, syncProtocol p.SyncProtocol) *Server {
	s := &Server{
		cnf:          cnf,
		support:      support,
		routing:      routing,
		tcpServer:    goetty.NewServer(cnf.Addr, p.DECODER, p.ENCODER, goetty.NewInt64IdGenerator()),
		handlers:     make(map[int32]CMDHandler),
		registor:     registor,
		syncProtocol: syncProtocol,

		tw: util.GetTimeWheel(),

		queue: make(chan *p.Message, cnf.WriteBufQueueSize),

		batch: make(map[int64]map[string]int64),
	}

	s.pool = c.NewConnectorPool(s.createGoettyConf, nil)

	go s.sendLoop()

	return s
}

func (self *Server) Registry(cmd int32, handler CMDHandler) {
	self.handlers[cmd] = handler
	log.Infof("%s cmd<%d, %s> registry added", util.MODULE_SERVICE, cmd, p.BaseCMD_name[cmd])
}

func (self *Server) Serve() error {
	err := self.registor.RegistorSupport(self.support, self.cnf.Etcd.RegisterTTL)

	if err != nil {
		return err
	}

	defer func() {
		self.registor.DeregistorSupport(self.support)
	}()

	return self.tcpServer.Serve(self.doServe)
}

func (self *Server) WriteTo(id string, msg *p.Message) error {
	offset, err := self.syncProtocol.Set(id, msg)
	if err != nil {
		return err
	}

	self.queue <- self.createSyncNotify(id, offset)
	return nil
}

func (self *Server) RawWriteTo(id string, msg *p.Message) error {
	self.queue <- msg
	return nil
}

func (self *Server) BeginBatch() int64 {
	token := tokenGen.Incr()
	self.batch[token] = make(map[string]int64)
	return token
}

func (self *Server) CommitBatch(token int64) {
	defer self.DeleteBatch(token)

	m := self.batch[token]
	if m == nil {
		return
	}

	for id, offset := range m {
		self.WriteTo(id, self.createSyncNotify(id, offset))
	}
}

func (self *Server) DeleteBatch(token int64) {
	delete(self.batch, token)
}

func (self *Server) AddToBatch(token int64, id string, msg *p.Message) error {
	m := self.batch[token]
	if m == nil {
		return nil
	}

	offset, err := self.syncProtocol.Set(id, msg)
	if err != nil {
		return err
	}

	m[id] = offset
	return nil
}

func (self *Server) GetSyncProtocol() p.SyncProtocol {
	return self.syncProtocol
}

func (self *Server) sendLoop() {
	for {
		w := <-self.queue

		addr, err := self.routing.GetRoutingBind(w.GetSessionId())
		if err != nil {
			log.InfoErrorf(err, "%s id<%s> get bind failure", util.MODULE_SERVICE, w.GetSessionId())
			continue
		}

		log.Infof("%s id<%s> binded router<%s>", util.MODULE_SERVICE, w.GetSessionId(), addr)

		conn, err := self.pool.GetConnector(addr)
		if err != nil {
			log.InfoErrorf(err, "%s id<%s> get bind failure", util.MODULE_SERVICE, w.GetSessionId())
			continue
		}

		err = conn.Write(w)
		if err != nil {
			log.InfoErrorf(err, "%s id<%s> write cmd<%d, %s> msg<%s> to router<%s> failure", util.MODULE_SERVICE, w.GetSessionId(), w.GetCmd(), p.BaseCMD_name[w.GetCmd()], w, addr)
		} else {
			log.Infof("%s id<%s> write cmd<%d, %s> msg<%s> to router<%s>", util.MODULE_SERVICE, w.GetSessionId(), w.GetCmd(), p.BaseCMD_name[w.GetCmd()], w, addr)
		}
	}
}

// never use defer
func (self *Server) doServe(session goetty.IOSession) error {
	log.Infof("%s new router<%s> connected", util.MODULE_SERVICE, session.RemoteAddr())

	var data interface{}
	var err error

	for {
		p.POOL.Put(data)
		data, err = session.Read()

		if err != nil {
			log.WarnErrorf(err, "%s read msg from router<%s> failure", util.MODULE_SERVICE, session.RemoteAddr())
			break
		}

		msg, _ := data.(*p.Message)
		cmd := msg.GetCmd()

		log.Infof("%s id<%s> cmd<%d, %s> read a msg<%+v> from <%s>", util.MODULE_SERVICE, msg.GetSessionId(), cmd, p.BaseCMD_name[cmd], msg, session.RemoteAddr())

		// ack to client
		if msg.GetCliSeqId() > 0 && cmd != int32(p.BaseCMD_REQ_SYNC) {
			log.Infof("%s id<%s> ack cmd<%d, %s>", util.MODULE_SERVICE, msg.GetSessionId(), cmd, p.BaseCMD_name[cmd])

			err := self.WriteTo(msg.GetSessionId(), CreateFrom(msg, int32(p.BaseCMD_ACK)))
			if err != nil {
				log.WarnErrorf(err, "%s id<%s> cmd<%d, %s> ack failure", util.MODULE_SERVICE, msg.GetSessionId(), cmd, p.BaseCMD_name[cmd])
			}
		} else {
			log.Infof("%s id<%s> not need ack cmd<%d, %s>", util.MODULE_SERVICE, msg.GetSessionId(), cmd, p.BaseCMD_name[cmd])
		}

		handler, ok := self.handlers[cmd]

		if !ok {
			log.Infof("%s id<%s> cmd<%d, %s> handler not found", util.MODULE_SERVICE, msg.GetSessionId(), cmd, p.BaseCMD_name[cmd])
		} else {
			err := handler.Handler(msg)
			if err != nil {
				log.WarnErrorf(err, "%s id<%s> cmd<%d, %s> handler failure", util.MODULE_SERVICE, msg.GetSessionId(), cmd, p.BaseCMD_name[cmd])
			}
		}
	}

	p.POOL.Put(data)
	return err
}

func (self *Server) createSyncNotify(id string, offset int64) *p.Message {
	rsp := &p.SyncNotify{}
	rsp.Offset = proto.Int64(offset)
	data, _ := proto.Marshal(rsp)

	msg := &p.Message{}
	msg.Cmd = proto.Int32(int32(p.BaseCMD_NTY_SYNC))
	msg.SessionId = proto.String(id)
	msg.MsgId = proto.String(CreateMsgId())
	msg.Data = data

	return msg
}

func (self *Server) createGoettyConf(addr string) *goetty.Conf {
	return &goetty.Conf{
		Addr:                   addr,
		TimeWheel:              self.tw,
		TimeoutRead:            self.cnf.Timeout.TimeoutRead,
		TimeoutWrite:           self.cnf.Timeout.TimeoutWrite,
		TimeoutConnectToServer: self.cnf.Timeout.TimeoutConnect,
		WriteTimeoutFn:         nil,
	}
}

func CreateMsgId() string {
	return goetty.NewV4UUID()
}

func CreateFrom(msg *p.Message, cmd int32) *p.Message {
	v := &p.Message{}
	v.Cmd = proto.Int32(cmd)
	v.SessionId = proto.String(msg.GetSessionId())
	v.MsgId = proto.String(CreateMsgId())
	v.CliSeqId = proto.Int32(msg.GetCliSeqId())

	return v
}
