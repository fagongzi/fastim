package bind

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/fagongzi/fastim/pkg/model"
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/fastim/pkg/registor"
	"github.com/fagongzi/fastim/pkg/util"
	"github.com/garyburd/redigo/redis"
	"sync"
	"time"
)

const (
	BUCKET_SUPPORT_SIZE = 64
	BIND_PREFIX         = "bind_"
)

var (
	ERR_BACKEND_NOT_MATCH = errors.New("backend not match")
	ERR_BIND_NOT_FOUND    = errors.New("bind not found")
)

type supportMap struct {
	sync.RWMutex
	counter  int64
	supports *list.List
}

func (self *supportMap) next() *model.Support {
	defer func() { self.counter += 1 }()
	index := int(self.counter % int64(self.supports.Len()))

	i := 0
	for iter := self.supports.Front(); iter != nil; iter = iter.Next() {
		if index == i {
			s, _ := iter.Value.(*model.Support)
			return s
		}
		i += 1
	}

	return nil
}

func NewRedisPool(servers []string, maxIdle int, idleTimeout time.Duration) *redis.Pool {
	counter := 0
	return &redis.Pool{
		MaxIdle:     maxIdle,
		IdleTimeout: idleTimeout,
		Dial: func() (redis.Conn, error) {
			defer func() { counter += 1 }()
			c, err := redis.Dial("tcp", servers[counter%len(servers)])
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

type RedisRouting struct {
	addr        string
	registor    registor.Registor
	supportsMap map[int]*supportMap

	watchStopCh    chan bool
	watchReceiveCh chan *registor.Evt

	pool *redis.Pool

	binding map[string]map[int32]*model.Support
}

func NewRedisRouting(addr string, pool *redis.Pool, reg registor.Registor, load bool) (Routing, error) {
	r := &RedisRouting{
		addr:        addr,
		registor:    reg,
		supportsMap: make(map[int]*supportMap, BUCKET_SUPPORT_SIZE),

		pool: pool,

		watchStopCh:    make(chan bool),
		watchReceiveCh: make(chan *registor.Evt),

		binding: make(map[string]map[int32]*model.Support),
	}

	for i := 0; i < BUCKET_SUPPORT_SIZE; i++ {
		r.supportsMap[i] = &supportMap{supports: list.New()}
	}

	if load {
		return r, r.init()
	} else {
		return r, nil
	}
}

func (self RedisRouting) Bind(id string, ex string) error {
	self.binding[id] = make(map[int32]*model.Support)

	conn := self.pool.Get()
	defer conn.Close()

	_, err := conn.Do("SET", fmt.Sprintf("%s%s", BIND_PREFIX, id), self.addr, "EX", ex)
	return err
}

func (self RedisRouting) Unbind(id string) error {
	delete(self.binding, id)

	conn := self.pool.Get()
	defer conn.Close()
	_, err := conn.Do("DELETE", fmt.Sprintf("%s%s", BIND_PREFIX, id))
	return err
}

func (self RedisRouting) SelectBackend(msg *p.Message) (string, error) {
	if support, ok := self.binding[msg.GetSessionId()][msg.GetBiz()]; ok {
		return support.Addr, nil
	}

	product := int(msg.GetProduct())
	m, ok := self.supportsMap[product%BUCKET_SUPPORT_SIZE]
	if !ok {
		return "", ERR_BACKEND_NOT_MATCH
	}

	m.RLock()
	defer m.RUnlock()

	for i := 0; i < m.supports.Len(); i++ {
		support := m.next()
		if support.Mathces(msg) {
			self.binding[msg.GetSessionId()][msg.GetBiz()] = support
			return support.Addr, nil
		}
	}

	return "", ERR_BACKEND_NOT_MATCH
}

func (self RedisRouting) GetRoutingBind(id string) (string, error) {
	conn := self.pool.Get()
	defer conn.Close()

	return redis.String(conn.Do("GET", fmt.Sprintf("%s%s", BIND_PREFIX, id)))
}

func (self RedisRouting) Watch() {
	go self.doEvtReceive()
	self.registor.Watch(self.watchReceiveCh, self.watchStopCh)
}

func (self *RedisRouting) init() error {
	supports, err := self.registor.GetSupports()

	if err != nil {
		return err
	}

	for _, support := range supports {
		self.addSupport(support)
	}

	return nil
}

func (self *RedisRouting) addSupport(support *model.Support) {
	product := support.Product

	m := self.supportsMap[product%BUCKET_SUPPORT_SIZE]
	m.Lock()
	defer m.Unlock()

	m.supports.PushBack(support)

	log.Infof("%s support <%+v> added.", util.MODULE_ROUTING, support)
}

func (self *RedisRouting) deleteSupport(support *model.Support) {
	product := support.Product

	m := self.supportsMap[product%BUCKET_SUPPORT_SIZE]
	m.Lock()
	defer m.Unlock()

	var e *list.Element

	for iter := m.supports.Front(); iter != nil; iter = iter.Next() {
		s, _ := iter.Value.(*model.Support)

		if s.Product == support.Product && s.Addr == support.Addr {
			e = iter
			break
		}
	}

	if nil != e {
		m.supports.Remove(e)
	}

	log.Infof("%s support <%+v> deleted.", util.MODULE_ROUTING, support)
}

func (self *RedisRouting) doEvtReceive() {
	for {
		evt := <-self.watchReceiveCh

		if evt.Src == registor.EVT_SRC_SUPPORT {
			self.doReceiveSupport(evt)
		}
	}
}

func (self *RedisRouting) doReceiveSupport(evt *registor.Evt) {
	support, _ := evt.Value.(*model.Support)

	if evt.Type == registor.EVT_TYPE_NEW {
		self.addSupport(support)
	} else if evt.Type == registor.EVT_TYPE_DELETE {
		self.deleteSupport(support)
	}
}
