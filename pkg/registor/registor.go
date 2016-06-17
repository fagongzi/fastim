package registor

import (
	"github.com/fagongzi/fastim/pkg/model"
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

	RegisterRouter(router *model.Router, ttl uint64) error
	DeregisterRouter(router *model.Router) error

	GetSupports() ([]*model.Support, error)

	RegistorWatchHandler(evtSrc EvtSrc, evtType EvtType, handler func(evt *Evt))
	Watch(stopCh chan bool) error
}
