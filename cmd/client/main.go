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
		Addr:                   *server,
		TimeWheel:              tw,
		TimeoutRead:            time.Second * 5,
		TimeoutWrite:           time.Second * 5,
		TimeoutConnectToServer: time.Second * 5,
	}, p.DECODER, p.ENCODER)

	_, err := c.Connect()
	if err != nil {
		fmt.Println(err)
		return
	}

	defer c.Close()

	m := &p.Message{}
	m.Cmd = proto.Int32(200)
	m.Biz = proto.Int32(1)
	m.Product = proto.Int32(1)
	m.ProtocolVersion = proto.Int32(1)
	m.CliVersion = proto.Int32(1)

	err = c.Write(m)
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		msg, err := c.Read()
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(msg)
	}
}
