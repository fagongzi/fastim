package router

import (
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/fagongzi/fastim/pkg/conf"
	"github.com/fagongzi/fastim/pkg/model"
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/fastim/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/golang/protobuf/proto"
	"time"
)

const (
	SINGLE = ""
)

type tcpTransport struct {
	session goetty.IOSession
	product int32
	writeCh chan *p.Message
	closeCh chan interface{}
}

func (t tcpTransport) Write(msg *p.Message) error {
	t.writeCh <- msg
	return nil
}

func (t tcpTransport) Close() error {
	t.closeCh <- SINGLE
	return nil
}

func (t tcpTransport) GetProduct() int32 {
	return t.product
}

func (t tcpTransport) Id() string {
	id, _ := t.session.Id().(string)
	return id
}

func (t tcpTransport) StartWriteLoop() {
	sid := t.Id()

	log.Infof("%s id<%s> write loop start", util.MODULE_FRONTEND_TCP, sid)

	for {
		select {
		case msg := <-t.writeCh:
			err := t.session.Write(msg)

			if err != nil {
				log.InfoErrorf(err, "%s id<%s> send msg<%s> to client<%s> failure", util.MODULE_FRONTEND_TCP, msg.GetSessionId(), msg.GetMsgId(), t.session.RemoteAddr())
			} else {
				log.Infof("%s id<%s> send msg<%s> to client<%s>", util.MODULE_FRONTEND_TCP, msg.GetSessionId(), msg.GetMsgId(), t.session.RemoteAddr())
			}

			p.POOL.Put(msg)
		case <-t.closeCh:
			log.Infof("%s id<%s> write loop closed", util.MODULE_FRONTEND_TCP, sid)
			close(t.closeCh)
			return
		}
	}
}

type TCPServer struct {
	cnf       *conf.RouterConf
	tcpServer *goetty.Server
	service   *Service
}

func NewTCPServer(cnf *conf.RouterConf, service *Service) *TCPServer {
	return &TCPServer{
		cnf:       cnf,
		service:   service,
		tcpServer: goetty.NewServer(cnf.Addr, p.DECODER, p.ENCODER, goetty.NewUUIDV4IdGenerator()),
	}
}

func (self *TCPServer) Serve() error {
	err := self.service.registerRouter(model.PROTOCOL_TCP)
	if err != nil {
		return err
	}

	defer self.service.deregisterRouter(model.PROTOCOL_TCP)

	return self.tcpServer.Serve(self.doServe)
}

func (self *TCPServer) Stop() {
	defer self.tcpServer.Stop()
	self.service.stop()
}

// client session, every session has 2 goroutines, one for read, one for write. read goroutine is the caller of this method
// manual process return, never use defer
func (self *TCPServer) doServe(session goetty.IOSession) error {
	// reject new connection
	if self.service.isStopping() {
		return self.reject(session, true)
	}

	var data interface{}
	var err error
	var transport Transport
	sid, _ := session.Id().(string)

	log.Infof("%s id<%s> allocate to client<%s>", util.MODULE_FRONTEND_TCP, sid, session.RemoteAddr())
	bind := false

	for {
		self.release(data)
		data, err = session.ReadTimeout(self.cnf.Timeout.TimeoutRead)

		// reject new msg, when server stopped
		if self.service.isStopping() {
			err = self.reject(session, false)
			break
		}

		if err != nil {
			log.WarnErrorf(err, "%s id<%s> IO error", util.MODULE_FRONTEND_TCP, sid)
			break
		}

		msg, _ := data.(*p.Message)
		msg.SessionId = proto.String(sid)

		if !bind {
			bind = true

			transport = &tcpTransport{
				session: session,
				product: msg.GetProduct(),
				writeCh: make(chan *p.Message, 1024),
				closeCh: make(chan interface{}),
			}

			self.service.addTransport(sid, transport)

			go transport.StartWriteLoop()
		}

		log.Infof("%s id<%s> read a msg <%+v>", util.MODULE_FRONTEND_TCP, sid, msg)

		err = self.service.doFromFrontend(msg)
		if err != nil {
			log.WarnErrorf(err, "%s id<%s> send to backend failure", util.MODULE_FRONTEND_TCP, sid)
			break
		}
	}

	self.release(data)
	if bind {
		self.releaseConnection(transport)
	}

	return err
}

func (self *TCPServer) reject(session goetty.IOSession, closed bool) error {
	log.Infof("%s server is stopping, reject connection<%s>", util.MODULE_FRONTEND_TCP, session.RemoteAddr())
	session.Write(self.service.shutdown)
	if closed {
		time.Sleep(time.Second * 2)
		return session.Close()
	}
	return nil
}

func (self *TCPServer) release(data interface{}) {
	if nil == data {
		return
	}
	p.POOL.Put(data)
}

func (self *TCPServer) releaseConnection(transport Transport) {
	self.service.deleteTransport(transport.Id())
	transport.Close()
}
