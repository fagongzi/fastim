package protocol

// sync protocol
// server -> client sync_notify
// client -> server sync_req
// server -> client sync_rsp
// server side use queue store messages, client use offset to get message.
// server received a sync message, delete messages which index is less than the client offset, and return the messages
// which index between offset+1 and offset+count
type SyncProtocol interface {
	Clear(id string) error
	Create(id string) error
	Rename(oldId string, newId string) error
	SetTimeout(id string, seconds int) error
	Set(id string, msgs ...*Message) (maxOffset int64, err error)
	Get(id string, count int64, offset int64) (messages []*Message, readedOffset int64, err error)
}
