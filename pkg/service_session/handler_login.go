package service_session

import (
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/fastim/pkg/service"
	proto "github.com/golang/protobuf/proto"
)

type LoginReqHandler struct {
	server *service.Server
}

func (self LoginReqHandler) Handler(msg *p.Message) error {
	req := &p.LoginReq{}
	err := proto.Unmarshal(msg.GetData(), req)
	if nil != err {
		return err
	}

	self.server.GetSyncProtocol().Create(msg.GetSessionId())

	rsp := &p.LoginRsp{}

	if !self.Valid(req) {
		rsp.Code = proto.Int32(1)
	}

	data, _ := proto.Marshal(rsp)

	m := service.CreateFrom(msg, int32(p.BaseCMD_RSP_LOGIN))
	m.Data = data

	return self.server.WriteTo(msg.GetSessionId(), m)
}

func (self *LoginReqHandler) Valid(req *p.LoginReq) bool {
	return req.GetName() == "admin" && req.GetPassword() == "admin"
}
