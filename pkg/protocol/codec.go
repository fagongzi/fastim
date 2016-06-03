package protocol

import (
	"github.com/fagongzi/goetty"
	proto "github.com/golang/protobuf/proto"
)

var (
	DECODER = goetty.NewIntLengthFieldBasedDecoder(NewProtobufDecoder())
	ENCODER = NewProtobufEncoder()
)

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

func NewProtobufDecoder() goetty.Decoder {
	return &ProtobufDecoder{}
}

func (self ProtobufDecoder) Decode(in *goetty.ByteBuf) (complete bool, msg interface{}, err error) {
	_, data, err := in.ReadMarkedBytes()

	if err != nil {
		return true, nil, err
	}

	pb := &Message{}
	err = proto.Unmarshal(data, pb)
	if err != nil {
		return true, nil, err
	}

	return true, pb, nil
}
