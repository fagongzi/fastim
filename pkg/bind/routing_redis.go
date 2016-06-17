package bind

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/CodisLabs/codis/pkg/utils/atomic2"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/fagongzi/fastim/pkg/model"
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/fastim/pkg/registor"
	"github.com/fagongzi/fastim/pkg/util"
	"github.com/garyburd/redigo/redis"
	"sync"
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
	counter  atomic2.Int64
	pCounter atomic2.Int64
	supports *list.List
}

func (self *supportMap) nextS(biz int, product int) *model.Support {
	self.RLock()
	defer self.RUnlock()

	// max retry: len(supports)
	maxRetry := self.supports.Len()

	for i := 0; i < maxRetry; i++ {
		support := self.nextIndex(int(self.pCounter.Incr() % int64(maxRetry)))
		if nil != support && support.Product == product && support.SupportBiz(biz) {
			return support
		}
	}

	return nil
}

func (self *supportMap) next() *model.Support {
	self.RLock()
	defer self.RUnlock()

	if self.supports.Len() == 0 {
		return nil
	}

	return self.nextIndex(int(self.counter.Incr() % int64(self.supports.Len())))
}

func (self *supportMap) nextIndex(index int) *model.Support {
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

type RedisRouting struct {
	mutex       *sync.RWMutex
	addr        string
	registor    registor.Registor
	supportsMap map[int]*supportMap
	routers     map[string]*model.Router

	pool *redis.Pool

	binding map[string]map[int32]*model.Support // key: session_id; value: map (key: bizId, value: support)
}

func NewRedisRouting(addr string, pool *redis.Pool, reg registor.Registor, load bool) (Routing, error) {
	r := &RedisRouting{
		mutex: &sync.RWMutex{},

		addr:        addr,
		registor:    reg,
		supportsMap: make(map[int]*supportMap, BUCKET_SUPPORT_SIZE),
		routers:     make(map[string]*model.Router),

		pool: pool,

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

	_, err := conn.Do("DEL", fmt.Sprintf("%s%s", BIND_PREFIX, id))
	return err
}

func (self RedisRouting) GetBackend(msg *p.Message) (string, error) {
	if support, ok := self.binding[msg.GetSessionId()][msg.GetBiz()]; ok {
		return support.Addr, nil
	}

	product := int(msg.GetProduct())
	m, ok := self.supportsMap[product%BUCKET_SUPPORT_SIZE]
	if !ok {
		return "", ERR_BACKEND_NOT_MATCH
	}

	support := m.next()
	if nil != support && support.Mathces(msg) {
		self.binding[msg.GetSessionId()][msg.GetBiz()] = support
		return support.Addr, nil
	}

	return "", ERR_BACKEND_NOT_MATCH
}

func (self RedisRouting) GetSessionBackend(product int32) string {
	m := self.supportsMap[int(product)%BUCKET_SUPPORT_SIZE]

	support := m.nextS(int(p.BaseBiz_SESSION), int(product))
	if nil != support {
		return support.Addr
	}

	return ""
}

func (self RedisRouting) GetRoutingBind(id string) (string, error) {
	conn := self.pool.Get()
	defer conn.Close()

	return redis.String(conn.Do("GET", fmt.Sprintf("%s%s", BIND_PREFIX, id)))
}

func (self RedisRouting) AddRouter(router *model.Router) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.routers[router.Addr] = router
	log.Infof("%s router <%+v> added.", util.MODULE_ROUTING, router)
}

func (self RedisRouting) DeleteRouter(router *model.Router) *model.Router {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	v := self.routers[router.Addr]
	delete(self.routers, router.Addr)
	log.Infof("%s router <%+v> deleted.", util.MODULE_ROUTING, router)
	return v
}

func (self RedisRouting) GetRouter(addr string) *model.Router {
	self.mutex.RLock()
	defer self.mutex.RUnlock()

	return self.routers[addr]
}

func (self RedisRouting) AddSupport(support *model.Support) {
	product := support.Product

	m := self.supportsMap[product%BUCKET_SUPPORT_SIZE]
	m.Lock()
	defer m.Unlock()

	m.supports.PushBack(support)

	log.Infof("%s support <%+v> added.", util.MODULE_ROUTING, support)
}

func (self RedisRouting) DeleteSupport(support *model.Support) {
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

func (self RedisRouting) BindSession(key string, id string, product int32) error {
	conn := self.pool.Get()
	defer conn.Close()

	_, err := conn.Do("SADD", key, fmt.Sprintf("%d:%s", product, id))
	return err
}

func (self RedisRouting) UnbindSession(key string, id string, product int32) error {
	conn := self.pool.Get()
	defer conn.Close()

	_, err := conn.Do("SREM", key, fmt.Sprintf("%d:%s", product, id))
	return err
}

func (self *RedisRouting) init() error {
	supports, err := self.registor.GetSupports()

	if err != nil {
		return err
	}

	for _, support := range supports {
		self.AddSupport(support)
	}

	return nil
}
