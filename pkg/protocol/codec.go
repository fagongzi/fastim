package protocol

import (
	"github.com/fagongzi/goetty"
	proto "github.com/golang/protobuf/proto"
	"sync"
)

var (
	POOL    = NewPool()
	DECODER = goetty.NewIntLengthFieldBasedDecoder(NewProtobufDecoder())
	ENCODER = NewProtobufEncoder()
)

type Pool struct {
	pool *sync.Pool
}

type ProtobufEncoder struct {
}

type ProtobufDecoder struct {
}

func NewProtobufEncoder() goetty.Encoder {
	return &ProtobufEncoder{}
}

func (self ProtobufEncoder) Encode(data interface{}, out *goetty.ByteBuf) error {
	msg, _ := data.(*Message)
	b, err := proto.Marshal(msg)

	if nil != err {
		return err
	}

	out.WriteInt(len(b))
	out.Write(b)

	return nil
}

func NewPool() *Pool {
	return &Pool{
		pool: &sync.Pool{},
	}
}

func (p *Pool) Get() *Message {
	v := p.pool.Get()
	if v == nil {
		return &Message{}
	}

	pb, _ := v.(*Message)
	return pb
}

func (p *Pool) Put(msg interface{}) {
	if nil == msg {
		return
	}

	pb, ok := msg.(*Message)
	if ok {
		pb.Reset()
		p.pool.Put(pb)
	}
}

func NewProtobufDecoder() goetty.Decoder {
	return &ProtobufDecoder{}
}

func (self ProtobufDecoder) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	_, data, err := in.ReadMarkedBytes()

	if err != nil {
		return true, nil, err
	}

	pb := POOL.Get()
	err = proto.Unmarshal(data, pb)
	if err != nil {
		return true, pb, err
	}

	return true, pb, nil
}
