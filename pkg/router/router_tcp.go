package router

import (
	"github.com/CodisLabs/codis/pkg/utils/log"
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/fastim/pkg/registor"
	"github.com/fagongzi/fastim/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/golang/protobuf/proto"
)

type tcpTransport struct {
	session  goetty.IOSession
	backends *Backends
}

func (t tcpTransport) WriteToFrontend(msg *p.Message) error {
	err := t.session.Write(msg)
	if err == nil {
		log.Infof("%s id<%s> send msg<%d> to client<%s>", util.MODULE_FRONTEND_TCP, msg.GetSessionId(), msg.GetSeqId(), t.session.RemoteAddr())
	}
	return err
}

func (t tcpTransport) WriteToBackend(addr string, msg *p.Message) error {
	return t.backends.SendTo(addr, msg)
}

type TCPServer struct {
	cnf       *Conf
	registor  registor.Registor
	tcpServer *goetty.Server
	service   *Service
	backends  *Backends
}

func NewTCPServer(cnf *Conf, registor registor.Registor, service *Service, backends *Backends) *TCPServer {
	return &TCPServer{
		cnf:       cnf,
		registor:  registor,
		service:   service,
		backends:  backends,
		tcpServer: goetty.NewServer(cnf.Addr, p.DECODER, p.ENCODER, goetty.NewUUIDV4IdGenerator()),
	}
}

func (self *TCPServer) Serve() error {
	err := self.registor.RegisterRouter(self.cnf.InternalAddr, registor.PROTOCOL_TCP, self.cnf.RegisterTTL)
	if err != nil {
		return err
	}

	defer func() {
		self.registor.DeregisterRouter(self.cnf.Addr, registor.PROTOCOL_TCP)
	}()

	return self.tcpServer.Serve(self.doServe)
}

func (self *TCPServer) doServe(session goetty.IOSession) error {
	sid, _ := session.Id().(string)

	log.Infof("%s id<%s> allocate to client<%s>", util.MODULE_FRONTEND_TCP, sid, session.RemoteAddr())

	self.service.addTransport(sid, &tcpTransport{
		session:  session,
		backends: self.backends,
	})

	defer self.service.deleteTransport(sid)

	for {
		data, err := session.Read()

		if err != nil {
			log.WarnErrorf(err, "%s id<%s> IO error", util.MODULE_FRONTEND_TCP, sid)
			return err
		}

		msg, _ := data.(*p.Message)
		msg.SessionId = proto.String(sid)

		log.Infof("%s id<%s> read a msg <%+v>", util.MODULE_FRONTEND_TCP, sid, msg)

		err = self.service.doFromFrontend(msg)
		if err != nil {
			log.WarnErrorf(err, "%s id<%s> send to backend failure", util.MODULE_FRONTEND_TCP, sid)
		}
	}

	return nil
}
