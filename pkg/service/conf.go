package service

import (
	"time"
)

type Conf struct {
	Addr        string
	EtcdAddrs   []string
	RedisAddrs  []string
	EtcdPrefix  string
	RegisterTTL uint64

	TimeoutConnectRouter  time.Duration
	TimeoutReadFromRouter time.Duration
	TimeoutWriteToRouter  time.Duration
	TimeoutIdle           time.Duration

	MaxIdle int
}
