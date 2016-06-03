package router

import (
	"time"
)

type Conf struct {
	Addr         string
	InternalAddr string
	EtcdAddrs    []string
	RedisAddrs   []string
	EtcdPrefix   string
	RegisterTTL  uint64

	TimeoutConnectBackend  time.Duration
	TimeoutReadFromBackend time.Duration
	TimeoutWriteToBackend  time.Duration
	TimeoutIdle            time.Duration

	MaxIdle int

	EnableEncrypt bool
}
