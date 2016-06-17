package protocol

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	proto "github.com/golang/protobuf/proto"
	"testing"
	"time"
)

var redisPool = &redis.Pool{
	MaxIdle:     10,
	IdleTimeout: time.Second * 10,
	Dial: func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", "192.168.70.41:6379")
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

const (
	id = "abc"
)

func TestSet(t *testing.T) {
	m := &Message{}
	m.Cmd = proto.Int32(10)

	protocol := NewRedisSyncProtocol(redisPool)
	defer protocol.Clear(id)

	index, err := protocol.Set(id, m)
	if err != nil {
		t.Error(err)
		return
	}

	if 1 != index {
		t.Errorf("err index:%d, expect 1", index)
		return
	}

	m = &Message{}
	m.Cmd = proto.Int32(20)
	index, err = protocol.Set(id, m)
	if err != nil {
		t.Error(err)
		return
	}

	if 2 != index {
		t.Errorf("err index:%d, expect 2", index)
		return
	}
}

func TestGet(t *testing.T) {
	protocol := NewRedisSyncProtocol(redisPool)
	defer protocol.Clear(id)

	protocol.Create(id)

	var maxOffset int64

	for i := 0; i < 10; i++ {
		m := &Message{}
		m.Cmd = proto.Int32(int32(i))
		maxOffset, _ = protocol.Set(id, m)
	}

	maxOffset -= 1

	var offset int64
	msgs, newOffset, err := protocol.Get(id, 1, offset)
	if err != nil {
		t.Error(err)
		return
	}

	if msgs == nil || len(msgs) != 1 {
		t.Errorf("get err. expect:%d", 1)
		return
	}

	if msgs[0].GetCmd() != 0 {
		t.Errorf("get err. expect:0, acture:%d", msgs[0].GetCmd())
		return
	}

	offset = newOffset

	fmt.Printf("newOffset:%d\n", newOffset)

	msgs, newOffset, err = protocol.Get(id, 1, offset)
	if err != nil {
		t.Error(err)
		return
	}

	if msgs == nil || len(msgs) != 1 {
		t.Errorf("get err. expect:%d", 1)
		return
	}

	if msgs[0].GetCmd() != 1 {
		t.Errorf("get err. expect:1, acture:%d", msgs[0].GetCmd())
		return
	}
}
