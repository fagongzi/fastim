package model

import (
	"encoding/json"
)

type RouterProtocol int

const (
	PROTOCOL_TCP              = RouterProtocol(0)
	PROTOCOL_HTTP_LONGPOLLING = RouterProtocol(1)
	PROTOCOL_HTTP_WEBSOCKET   = RouterProtocol(2)
)

type Router struct {
	Addr         string         `json:"addr,omitempty"`
	InternalAddr string         `json:"internalAddr,omitempty"`
	Protocol     RouterProtocol `json:"protolcol,omitempty"`
	BucketSize   int            `json:"bucketSize,omitempty"`
}

func UnMarshalRouter(data []byte) *Router {
	v := &Router{}
	json.Unmarshal(data, v)

	return v
}

func (self *Router) Marshal() []byte {
	v, _ := json.Marshal(self)
	return v
}
