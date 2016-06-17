package service_session

import (
	"github.com/fagongzi/fastim/pkg/conf"
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/fastim/pkg/service"
)

type SessionClosedHandler struct {
	cnf    *conf.ServiceConf
	server *service.Server
}

func (self SessionClosedHandler) Handler(msg *p.Message) error {
	id := msg.GetSessionId()
	err := self.server.GetSyncProtocol().SetTimeout(id, self.cnf.DelaySessionClose)
	if nil != err {
		return err
	}

	return self.server.RawWriteTo(id, service.CreateFrom(msg, int32(p.RouterCMD_SESSION_CLOSED_COMPLETE)))
}
