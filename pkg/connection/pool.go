package connection

import (
	"github.com/CodisLabs/codis/pkg/utils/log"
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/fastim/pkg/util"
	"github.com/fagongzi/goetty"
	"sync"
	"time"
)

type ConnectorPool struct {
	sync.RWMutex
	confCreateFn func(string) *goetty.Conf
	loopFn       func(string, *goetty.Connector)
	connectors   map[string]*goetty.Connector
}

func NewConnectorPool(confCreateFn func(string) *goetty.Conf, loopFn func(string, *goetty.Connector)) *ConnectorPool {
	tw := goetty.NewHashedTimeWheel(time.Millisecond, 60, 3)
	tw.Start()

	return &ConnectorPool{
		confCreateFn: confCreateFn,
		loopFn:       loopFn,
		connectors:   make(map[string]*goetty.Connector),
	}
}

func (self *ConnectorPool) GetConnector(addr string) (*goetty.Connector, error) {
	conn := self.doGet(addr)

	if nil != conn {
		return conn, nil
	}

	err := self.createConnector(addr)

	if err != nil {
		return nil, err
	}

	return self.doGet(addr), nil
}

func (self *ConnectorPool) CloseConnector(addr string, conn *goetty.Connector) {
	self.Lock()
	defer self.Unlock()

	conn.Close()

	delete(self.connectors, addr)

	log.Infof("%s conn<%s> closed.", util.MODULE_CONNECTION, addr)
}

func (self *ConnectorPool) doGet(addr string) *goetty.Connector {
	self.RLock()
	defer self.RUnlock()

	return self.connectors[addr]
}

func (self *ConnectorPool) createConnector(addr string) error {
	self.Lock()
	defer self.Unlock()

	log.Debugf("%s start create conn<%s>", util.MODULE_CONNECTION, addr)

	if _, ok := self.connectors[addr]; ok {
		log.Debugf("%s conn<%s> is already created.", util.MODULE_CONNECTION, addr)
		return nil
	}

	connector := goetty.NewConnector(self.confCreateFn(addr), p.DECODER, p.ENCODER)
	_, err := connector.Connect()

	if err != nil {
		return err
	}

	log.Infof("%s conn<%s> created.", util.MODULE_CONNECTION, addr)

	self.connectors[addr] = connector

	if self.loopFn != nil {
		go self.loopFn(addr, connector)
	} else {
		go func(addr string, conn *goetty.Connector) {
			for {
				_, err := conn.Read()
				if err != nil {
					self.CloseConnector(addr, conn)
					return
				}
			}
		}(addr, connector)
	}

	return nil
}
