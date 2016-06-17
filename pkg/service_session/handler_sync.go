package service_session

import (
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/fastim/pkg/service"
	proto "github.com/golang/protobuf/proto"
)

type SyncHandler struct {
	server *service.Server
}

func (self SyncHandler) Handler(msg *p.Message) error {
	req := &p.SyncReq{}
	err := proto.Unmarshal(msg.Data, req)
	if err != nil {
		return err
	}

	msgs, offset, err := self.server.GetSyncProtocol().Get(msg.GetSessionId(), int64(req.GetCount()), req.GetOffset())
	if err != nil {
		return err
	}

	rsp := &p.SyncRsp{}
	rsp.Messages = msgs
	rsp.Offset = proto.Int64(offset)
	data, _ := proto.Marshal(rsp)

	m := service.CreateFrom(msg, int32(p.BaseCMD_RSP_SYNC))
	m.Data = data

	return self.server.RawWriteTo(msg.GetSessionId(), m)
}
