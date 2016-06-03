package service_session

import (
	"github.com/fagongzi/fastim/pkg/bind"
	"github.com/fagongzi/fastim/pkg/model"
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/fastim/pkg/registor"
	"github.com/fagongzi/fastim/pkg/service"
)

type SessionService struct {
	cnf    *service.Conf
	server *service.Server
}

func NewSessionService(cnf *service.Conf, support *model.Support, routing bind.Routing, registor registor.Registor) *SessionService {
	return &SessionService{
		cnf:    cnf,
		server: service.NewServer(cnf, support, routing, registor),
	}
}

func (self *SessionService) Serve() error {
	self.server.Registry(200, &EchoHandler{service: self})
	return self.server.Serve()
}

func (self *SessionService) WriteTo(id string, msg *p.Message) error {
	return self.server.WriteTo(id, msg)
}
