package service

import (
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/fagongzi/fastim/pkg/bind"
	c "github.com/fagongzi/fastim/pkg/connection"
	"github.com/fagongzi/fastim/pkg/model"
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/fastim/pkg/registor"
	"github.com/fagongzi/fastim/pkg/util"
	"github.com/fagongzi/goetty"
	_ "github.com/golang/protobuf/proto"
)

var counter int64

type CMDHandler interface {
	Handler(msg *p.Message) error
}

type Server struct {
	cnf     *Conf
	support *model.Support

	tcpServer *goetty.Server

	tw   *goetty.HashedTimeWheel
	pool *c.ConnectorPool

	registor registor.Registor
	routing  bind.Routing

	handlers map[int]CMDHandler
}

func NewServer(cnf *Conf, support *model.Support, routing bind.Routing, registor registor.Registor) *Server {
	s := &Server{
		cnf:       cnf,
		support:   support,
		routing:   routing,
		tcpServer: goetty.NewServer(cnf.Addr, p.DECODER, p.ENCODER, goetty.NewInt64IdGenerator()),
		handlers:  make(map[int]CMDHandler),
		registor:  registor,

		tw: util.GetTimeWheel(),
	}

	s.pool = c.NewConnectorPool(s.createGoettyConf, nil)

	return s
}

func (self *Server) Registry(cmd int, handler CMDHandler) {
	self.handlers[cmd] = handler
}

func (self *Server) Serve() error {
	err := self.registor.RegistorSupport(self.support, self.cnf.RegisterTTL)

	if err != nil {
		return err
	}

	defer func() {
		self.registor.DeregistorSupport(self.support)
	}()

	return self.tcpServer.Serve(self.doServe)
}

func (self *Server) WriteTo(id string, msg *p.Message) error {
	addr, err := self.routing.GetRoutingBind(id)
	if err != nil {
		return err
	}

	log.Infof("%s id<%s> binded router<%s>", util.MODULE_SERVICE, id, addr)

	conn, err := self.pool.GetConnector(addr)
	if err != nil {
		return err
	}

	return conn.Write(msg)
}

func (self *Server) doServe(session goetty.IOSession) error {
	log.Infof("%s new router<%s> connected", util.MODULE_SERVICE, session.RemoteAddr())

	for {
		data, err := session.Read()

		if err != nil {
			log.WarnErrorf(err, "%s read msg from router<%s> failure", util.MODULE_SERVICE, session.RemoteAddr())
			return err
		}

		msg, _ := data.(*p.Message)

		log.Infof("%s id<%s> read a msg <%+v> from router<%s>", util.MODULE_SERVICE, msg.GetSessionId(), msg, session.RemoteAddr())

		cmd := int(msg.GetCmd())
		handler, ok := self.handlers[cmd]
		if !ok {
			log.Infof("%s id<%s> cmd<%d> handler not found", util.MODULE_SERVICE, msg.GetSessionId(), cmd)
		} else {
			err := handler.Handler(msg)
			if err != nil {
				log.InfoErrorf(err, "%s id<%s> cmd<%d> handler failure", util.MODULE_SERVICE, msg.GetSessionId(), cmd)
			}
		}
	}

	return nil
}

func (self *Server) createGoettyConf(addr string) *goetty.Conf {
	return &goetty.Conf{
		Addr:                   addr,
		TimeWheel:              self.tw,
		TimeoutRead:            self.cnf.TimeoutReadFromRouter,
		TimeoutWrite:           self.cnf.TimeoutWriteToRouter,
		TimeoutConnectToServer: self.cnf.TimeoutConnectRouter,
		WriteTimeoutFn:         nil,
	}
}
