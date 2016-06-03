package service_session

import (
	p "github.com/fagongzi/fastim/pkg/protocol"
)

type EchoHandler struct {
	service *SessionService
}

func (self EchoHandler) Handler(msg *p.Message) error {
	return self.service.WriteTo(msg.GetSessionId(), msg)
}
