package router

import (
	"github.com/CodisLabs/codis/pkg/utils/log"
	c "github.com/fagongzi/fastim/pkg/connection"
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/fastim/pkg/util"
	"github.com/fagongzi/goetty"
	"sync"
)

type Backends struct {
	sync.RWMutex
	cnf     *Conf
	service *Service
	server  *goetty.Server
	tw      *goetty.HashedTimeWheel
	pool    *c.ConnectorPool
}

func NewBackends(cnf *Conf, service *Service) *Backends {
	b := &Backends{
		cnf:     cnf,
		tw:      util.GetTimeWheel(),
		service: service,
		server:  goetty.NewServer(cnf.InternalAddr, p.DECODER, p.ENCODER, goetty.NewInt64IdGenerator()),
	}

	b.pool = c.NewConnectorPool(b.createGoettyConf, nil)

	go b.Serve()

	return b
}

func (self *Backends) SendTo(addr string, msg *p.Message) error {
	conn, err := self.pool.GetConnector(addr)

	if err != nil {
		log.InfoErrorf(err, "%s id<%d> connect to <%s> failure.", util.MODULE_BACKEND, msg.GetSessionId())
		return err
	}

	if nil == conn {
		return goetty.IllegalStateErr
	}

	log.Infof("%s id<%s> write <%+v> to <%s>", util.MODULE_BACKEND, msg.GetSessionId(), msg, addr)

	return conn.Write(msg)
}

func (self *Backends) Serve() {
	err := self.server.Serve(self.doServe)
	if nil != err {
		log.PanicErrorf(err, "%s start backends listener<%s> failure.", util.MODULE_BACKEND, self.cnf.InternalAddr)
	}
}

func (self *Backends) doServe(session goetty.IOSession) error {
	for {
		data, err := session.Read()
		if err != nil {
			log.InfoErrorf(err, "%s read from backend<%s> failure.", util.MODULE_BACKEND, session.RemoteAddr())
			return err
		} else {
			msg, _ := data.(*p.Message)

			log.Infof("%s id<%s> read a msg<%+v> from backend<%s>", util.MODULE_BACKEND, msg.GetSessionId(), msg, session.RemoteAddr())

			err = self.service.doFromBackend(msg)
			if err != nil {
				log.WarnErrorf(err, "%s id<%d> send msg<%d> to frontend failure", util.MODULE_BACKEND, msg.GetSessionId(), msg.GetSeqId())
			}
		}
	}

	return nil
}

func (self *Backends) writeTimeout(addr string, conn *goetty.Connector) {
	log.Warnf("%s conn<%s> write timeout.", util.MODULE_BACKEND, addr)
	// TODO: write heartbeat
}

func (self *Backends) createGoettyConf(addr string) *goetty.Conf {
	return &goetty.Conf{
		Addr:                   addr,
		TimeWheel:              self.tw,
		TimeoutRead:            self.cnf.TimeoutReadFromBackend,
		TimeoutWrite:           self.cnf.TimeoutWriteToBackend,
		TimeoutConnectToServer: self.cnf.TimeoutConnectBackend,
		WriteTimeoutFn:         self.writeTimeout,
	}
}
