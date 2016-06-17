package protocol

import (
	"fmt"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/fagongzi/fastim/pkg/util"
	"github.com/garyburd/redigo/redis"
	proto "github.com/golang/protobuf/proto"
	"strconv"
)

const (
	KEY_TTL = 3600 * 24 * 365 // one year
)

const (
	SYNC_ID_PREFIX          = "sync_id_"
	SYNC_PREFIX             = "sync_"
	SYNC_HEAD_OFFSET_PREFIX = "sync_head_offset_"
)

type RedisSyncProtocol struct {
	pool *redis.Pool
}

func NewRedisSyncProtocol(pool *redis.Pool) SyncProtocol {
	return &RedisSyncProtocol{
		pool: pool,
	}
}

func (self RedisSyncProtocol) Create(id string) error {
	conn := self.pool.Get()
	defer conn.Close()

	headOffsetKey := fmt.Sprintf("%s%s", SYNC_HEAD_OFFSET_PREFIX, id)
	_, err := conn.Do("SET", headOffsetKey, "0")

	return err
}

func (self RedisSyncProtocol) Rename(oldId string, newId string) error {
	oldIdKey := fmt.Sprintf("%s%s", SYNC_ID_PREFIX, oldId)
	newIdKey := fmt.Sprintf("%s%s", SYNC_ID_PREFIX, newId)

	oldSyncKey := fmt.Sprintf("%s%s", SYNC_PREFIX, oldId)
	newSyncKey := fmt.Sprintf("%s%s", SYNC_PREFIX, newId)

	oldOffsetKey := fmt.Sprintf("%s%s", SYNC_HEAD_OFFSET_PREFIX, oldId)
	newOffsetKey := fmt.Sprintf("%s%s", SYNC_HEAD_OFFSET_PREFIX, newId)

	conn := self.pool.Get()
	defer conn.Close()

	err := conn.Send("RENAME", oldIdKey, newIdKey)
	if err != nil {
		log.DebugErrorf(err, "%s RENAME <%s> to <%s> failure", util.MODULE_SERVICE, oldIdKey, newIdKey)
		return err
	}

	err = conn.Send("EXPIRE", newIdKey, KEY_TTL)
	if err != nil {
		log.DebugErrorf(err, "%s Update <%s> ttl failure", util.MODULE_SERVICE, newIdKey)
		return err
	}

	err = conn.Send("RENAME", oldSyncKey, newSyncKey)
	if err != nil {
		log.DebugErrorf(err, "%s RENAME <%s> to <%s> failure", util.MODULE_SERVICE, oldSyncKey, newSyncKey)
		return err
	}

	err = conn.Send("EXPIRE", newSyncKey, KEY_TTL)
	if err != nil {
		log.DebugErrorf(err, "%s Update <%s> ttl failure", util.MODULE_SERVICE, newSyncKey)
		return err
	}

	err = conn.Send("RENAME", oldOffsetKey, newOffsetKey)
	if err != nil {
		log.DebugErrorf(err, "%s RENAME <%s> to <%s> failure", util.MODULE_SERVICE, oldOffsetKey, newOffsetKey)
		return err
	}

	err = conn.Send("EXPIRE", newOffsetKey, KEY_TTL)
	if err != nil {
		log.DebugErrorf(err, "%s Update <%s> ttl failure", util.MODULE_SERVICE, newOffsetKey)
		return err
	}

	err = conn.Flush()
	if err != nil {
		log.DebugErrorf(err, "%s RENAME flush failure", util.MODULE_SERVICE)
		return err
	}

	for i := 0; i < 6; i++ {
		_, err := conn.Receive()
		if err != nil {
			log.DebugErrorf(err, "%s RENAME receive failure", util.MODULE_SERVICE)
			return err
		}
	}

	return nil
}

func (self RedisSyncProtocol) Set(id string, msgs ...*Message) (int64, error) {
	conn := self.pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("%s%s", SYNC_ID_PREFIX, id)
	msgKey := fmt.Sprintf("%s%s", SYNC_PREFIX, id)

	err := conn.Send("INCR", key)
	if err != nil {
		log.DebugErrorf(err, "%s INCR key<%s> failure", util.MODULE_SERVICE, key)
		return 0, err
	}

	for _, msg := range msgs {
		data, _ := proto.Marshal(msg)
		err = conn.Send("RPUSH", msgKey, data)
		if err != nil {
			log.DebugErrorf(err, "%s RPUSH key<%s> data<%s> failure", util.MODULE_SERVICE, msgKey, msg)
			return 0, err
		}
	}

	err = conn.Flush()
	if err != nil {
		log.DebugErrorf(err, "%s flush failure", util.MODULE_SERVICE)
		return 0, err
	}

	offset, err := redis.Int64(conn.Receive())
	if err != nil {
		log.DebugErrorf(err, "%s receive offset failure", util.MODULE_SERVICE)
		return 0, err
	}

	for i := 0; i < len(msgs); i++ {
		_, err = conn.Receive()
		if err != nil {
			log.DebugErrorf(err, "%s receive msg failure", util.MODULE_SERVICE)
			return 0, err
		}
	}

	return offset, nil
}

func (self RedisSyncProtocol) Get(id string, count int64, offset int64) ([]*Message, int64, error) {
	conn := self.pool.Get()
	defer conn.Close()

	msgKey := fmt.Sprintf("%s%s", SYNC_PREFIX, id)
	headOffsetKey := fmt.Sprintf("%s%s", SYNC_HEAD_OFFSET_PREFIX, id)

	v, err := redis.String(conn.Do("GET", headOffsetKey))
	if err != nil {
		log.DebugErrorf(err, "%s Get headOffsetKey<%s> failure", util.MODULE_SERVICE, headOffsetKey)
		return nil, 0, err
	}

	headOffset, _ := strconv.ParseInt(v, 10, 0)

	from := offset - headOffset

	err = conn.Send("SET", headOffsetKey, offset)
	if err != nil {
		log.DebugErrorf(err, "%s Set headOffsetKey<%s> offset<%d> failure", util.MODULE_SERVICE, headOffsetKey, offset)
		return nil, 0, err
	}

	err = conn.Send("LRANGE", msgKey, from, from+count-1)
	if err != nil {
		log.DebugErrorf(err, "%s LRANGE key<%s> from<%d> to<%d> failure", util.MODULE_SERVICE, msgKey, from, from+count-1)
		return nil, 0, err
	}

	err = conn.Send("LTRIM", msgKey, from, "-1")
	if err != nil {
		log.DebugErrorf(err, "%s LTRIM key<%s> failure", util.MODULE_SERVICE, msgKey)
		return nil, 0, err
	}

	err = conn.Flush()
	if err != nil {
		log.DebugErrorf(err, "%s Flush failure", util.MODULE_SERVICE)
		return nil, 0, err
	}

	_, err = conn.Receive()
	if err != nil {
		log.DebugErrorf(err, "%s Receive failure", util.MODULE_SERVICE)
		return nil, 0, err
	}

	data, err := redis.ByteSlices(conn.Receive())

	if err != nil {
		log.DebugErrorf(err, "%s Receive msg failure", util.MODULE_SERVICE)
		return nil, 0, err
	}

	_, err = conn.Receive()
	if err != nil {
		log.DebugErrorf(err, "%s Receive trim failure", util.MODULE_SERVICE)
		return nil, 0, err
	}

	l := len(data)
	msgs := make([]*Message, l)
	for index, v := range data {
		msgs[index] = &Message{}
		proto.Unmarshal(v, msgs[index])
	}

	return msgs, offset + int64(l), nil
}

func (self RedisSyncProtocol) SetTimeout(id string, seconds int) error {
	conn := self.pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("%s%s", SYNC_ID_PREFIX, id)
	msgKey := fmt.Sprintf("%s%s", SYNC_PREFIX, id)
	currentKey := fmt.Sprintf("%s%s", SYNC_HEAD_OFFSET_PREFIX, id)

	err := conn.Send("EXPIRE", key, seconds)
	if nil != err {
		return nil
	}

	err = conn.Send("EXPIRE", msgKey, seconds)
	if nil != err {
		return nil
	}

	err = conn.Send("EXPIRE", currentKey, seconds)
	if nil != err {
		return nil
	}

	err = conn.Flush()
	if nil != err {
		return nil
	}

	for i := 0; i < 3; i++ {
		_, err = conn.Receive()
		if nil != err {
			return nil
		}
	}

	return nil
}

func (self RedisSyncProtocol) Clear(id string) error {
	key := fmt.Sprintf("%s%s", SYNC_ID_PREFIX, id)
	msgKey := fmt.Sprintf("%s%s", SYNC_PREFIX, id)
	currentKey := fmt.Sprintf("%s%s", SYNC_HEAD_OFFSET_PREFIX, id)

	conn := self.pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key, msgKey, currentKey)
	return err
}
