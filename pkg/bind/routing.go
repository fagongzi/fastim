package bind

import (
	p "github.com/fagongzi/fastim/pkg/protocol"
)

type Routing interface {
	Bind(id string, ex string) error
	Unbind(id string) error

	Watch()

	GetRoutingBind(id string) (string, error)

	SelectBackend(msg *p.Message) (string, error)
}
