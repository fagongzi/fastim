package util

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

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
