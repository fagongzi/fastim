package bind

import (
	"github.com/fagongzi/fastim/pkg/model"
	p "github.com/fagongzi/fastim/pkg/protocol"
)

type Routing interface {
	Bind(id string, ex string) error
	Unbind(id string) error

	AddSupport(support *model.Support)
	DeleteSupport(support *model.Support) //TODO: remove when backends loop error

	AddRouter(router *model.Router)
	DeleteRouter(router *model.Router) *model.Router
	GetRouter(addr string) *model.Router

	GetRoutingBind(id string) (string, error)

	GetBackend(msg *p.Message) (string, error) //TODO: retry
	GetSessionBackend(product int32) string    //TODO: impl

	BindSession(key string, id string, product int32) error
	UnbindSession(key string, id string, product int32) error
}
