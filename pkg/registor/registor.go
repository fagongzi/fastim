package registor

import (
	"github.com/fagongzi/fastim/pkg/model"
)

const (
	PROTOCOL_TCP              = "tcp"
	PROTOCOL_HTTP_LONGPOLLING = "longpolling"
	PROTOCOL_HTTP_WEBSOCKET   = "websocket"
)

type EvtType int
type EvtSrc int

const (
	EVT_TYPE_NEW    = EvtType(0)
	EVT_TYPE_UPDATE = EvtType(1)
	EVT_TYPE_DELETE = EvtType(2)
)

const (
	EVT_SRC_SUPPORT = EvtSrc(0)
	EVT_SRC_ROUTER  = EvtSrc(1)
)

type Evt struct {
	Src   EvtSrc
	Type  EvtType
	Key   string
	Value interface{}
}

type Registor interface {
	RegistorSupport(support *model.Support, ttl uint64) error
	DeregistorSupport(support *model.Support) error

	RegisterRouter(addr, protolcol string, ttl uint64) error
	DeregisterRouter(addr, protolcol string) error

	GetSupports() ([]*model.Support, error)

	Watch(evtCh chan *Evt, stopCh chan bool) error
}
