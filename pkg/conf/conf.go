package conf

import (
	"time"
)

type EtcdConf struct {
	EtcdAddrs   []string
	EtcdPrefix  string
	RegisterTTL uint64
}

type RedisConf struct {
	RedisAddrs  []string
	TimeoutIdle time.Duration
	MaxIdle     int
}

type TimeoutConf struct {
	TimeoutRead    time.Duration
	TimeoutConnect time.Duration
	TimeoutWrite   time.Duration
}

type RouterConf struct {
	Etcd    *EtcdConf
	Redis   *RedisConf
	Timeout *TimeoutConf

	Addr         string
	InternalAddr string

	BucketSize int // save session ids in the bucket, how many bukets in system
	MaxRetry   int

	TimeoutWaitAck time.Duration

	EnableEncrypt bool
}

type ServiceConf struct {
	Addr string

	WriteBufQueueSize int

	Etcd    *EtcdConf
	Redis   *RedisConf
	Timeout *TimeoutConf

	DelaySessionClose int
}
