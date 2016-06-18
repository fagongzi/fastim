package main

import (
	"flag"
	"fmt"
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/goetty"
	proto "github.com/golang/protobuf/proto"
	"time"
)

var (
	server = flag.String("server", "127.0.0.1:443", "listen msg from client.(e.g. ip:port)")
)

func main() {
	flag.Parse()

	tw := goetty.NewHashedTimeWheel(time.Second, 60, 2)
	tw.Start()

	c := goetty.NewConnector(&goetty.Conf{
		Addr: *server,
		TimeoutConnectToServer: time.Second * 5,
	}, p.DECODER, p.ENCODER)

	_, err := c.Connect()
	if err != nil {
		fmt.Println(err)
		return
	}

	defer c.Close()

	var maxOffset int64
	var offset int64
	syncing := false

	req := &p.LoginReq{}
	req.Name = proto.String("admin")
	req.Password = proto.String("admin")
	req.Equiment = p.Equiment_PC.Enum()
	req.Status = p.Status_ONLINE.Enum()
	data, _ := proto.Marshal(req)

	m := &p.Message{}
	m.Cmd = proto.Int32(int32(p.BaseCMD_REQ_LOGIN))
	m.Biz = proto.Int32(1)
	m.Product = proto.Int32(1)
	m.ProtocolVersion = proto.Int32(1)
	m.CliVersion = proto.Int32(1)
	m.CliSeqId = proto.Int32(1)
	m.Data = data

	err = c.Write(m)
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		data, err := c.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		msg, _ := data.(*p.Message)
		cmd := msg.GetCmd()

		fmt.Println(p.BaseCMD_name[cmd])

		if cmd == int32(p.BaseCMD_NTY_SYNC) {
			rsp := &p.SyncNotify{}
			proto.Unmarshal(msg.GetData(), rsp)

			maxOffset = rsp.GetOffset()

			fmt.Printf("max-offset is: %d, offset is: %d\n", maxOffset, offset)

			if offset >= maxOffset {
				continue
			}

			if syncing {
				continue
			}

			syncing = true

			req := &p.SyncReq{}
			req.Count = proto.Int32(10)
			req.Offset = proto.Int64(offset)
			bytes, _ := proto.Marshal(req)

			m := &p.Message{}
			m.Cmd = proto.Int32(int32(p.BaseCMD_REQ_SYNC))
			m.Biz = proto.Int32(1)
			m.Product = proto.Int32(1)
			m.ProtocolVersion = proto.Int32(1)
			m.CliVersion = proto.Int32(1)
			m.Data = bytes

			fmt.Printf("0-sync offset is: %d\n", offset)
			c.Write(m)
		} else if cmd == int32(p.BaseCMD_RSP_SYNC) {
			rsp := &p.SyncRsp{}
			proto.Unmarshal(msg.GetData(), rsp)

			for _, r := range rsp.Messages {
				fmt.Println(p.BaseCMD_name[r.GetCmd()])
			}

			offset = rsp.GetOffset()
			syncing = false

			if offset < maxOffset {
				syncing = true

				req := &p.SyncReq{}
				req.Count = proto.Int32(10)
				req.Offset = proto.Int64(offset)
				bytes, _ := proto.Marshal(req)

				m := &p.Message{}
				m.Cmd = proto.Int32(int32(p.BaseCMD_REQ_SYNC))
				m.Biz = proto.Int32(1)
				m.Product = proto.Int32(1)
				m.ProtocolVersion = proto.Int32(1)
				m.CliVersion = proto.Int32(1)
				m.Data = bytes

				fmt.Printf("1-sync offset is: %d\n", offset)
				c.Write(m)
			}
		}

	}
}
