package router

import (
	"github.com/CodisLabs/codis/pkg/utils/log"
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/fastim/pkg/util"
)

func (self *Service) bindRouterCMDHandlers() {
	self.routerHandlers[int32(p.RouterCMD_SESSION_CLOSED_COMPLETE)] = self.handlerSessionClosedComplete
}

// when backend complete session closed process, unbind the session.
// otherwise, sessin bind cache in redis, if the router crash, other router can continue send sessionClosed to backend
func (self *Service) handlerSessionClosedComplete(msg *p.Message) error {
	id := msg.GetSessionId()

	util.GetTimeWheel().Cancel(getIdTimeoutKey(id))

	transport := self.getTransport(id)
	if transport == nil || transport.GetProduct() <= 0 {
		return nil
	}

	tm := self.getTransportMap(id)

	if nil != tm {
		tm.Lock()
		defer tm.Unlock()

		delete(tm.transports, id)

		log.Infof("%s id<%s> unbind", util.MODULE_SERVICE, id)
		err := self.routing.Unbind(id)
		if err != nil {
			log.Infof("%s id<%s> unbind router failure", util.MODULE_SERVICE, id)
			return err
		}

		err = self.routing.UnbindSession(tm.key, id, transport.GetProduct())
		if err != nil {
			log.Infof("%s id<%s> unbind session failure", util.MODULE_SERVICE, id)
			return err
		}
	}

	return nil
}
