package service_session

import (
	"github.com/fagongzi/fastim/pkg/conf"
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/fastim/pkg/service"
)

type SessionService struct {
	cnf    *conf.ServiceConf
	server *service.Server
}

func NewSessionService(cnf *conf.ServiceConf, server *service.Server) *SessionService {
	return &SessionService{
		cnf:    cnf,
		server: server,
	}
}

func (self *SessionService) Serve() error {
	self.server.Registry(int32(p.RouterCMD_SESSION_CLOSED), &SessionClosedHandler{server: self.server, cnf: self.cnf})
	self.server.Registry(int32(p.BaseCMD_REQ_SYNC), &SyncHandler{server: self.server})
	self.server.Registry(int32(p.BaseCMD_REQ_LOGIN), &LoginReqHandler{server: self.server})
	return self.server.Serve()
}
