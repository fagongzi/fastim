package model

import (
	"encoding/json"
	p "github.com/fagongzi/fastim/pkg/protocol"
)

type Support struct {
	Addr        string `json:"addr,omitempty"`
	Product     int    `json:"product,omitempty"`
	CmdList     []int  `json:"cmdList,omitempty"`
	BizList     []int  `json:"bizList,omitempty"`
	MinProtocol int    `json:"minProtocol,omitempty"`
	MaxProtocol int    `json:"maxProtocol,omitempty"`
}

func UnMarshalSupport(data []byte) *Support {
	v := &Support{}
	json.Unmarshal(data, v)

	return v
}

func (self *Support) Marshal() []byte {
	v, _ := json.Marshal(self)
	return v
}

func (self *Support) Mathces(msg *p.Message) bool {
	return self.productMatches(msg) &&
		self.protocolMatches(msg) &&
		self.bizMatches(msg) &&
		self.cmdMatches(msg)
}

func (self *Support) productMatches(msg *p.Message) bool {
	return int(msg.GetProduct()) == self.Product
}

func (self *Support) bizMatches(msg *p.Message) bool {
	biz := int(msg.GetBiz())
	return self.BizList == nil || inArray(self.BizList, biz)
}

func (self *Support) cmdMatches(msg *p.Message) bool {
	cmd := int(msg.GetCmd())
	return self.CmdList == nil || inArray(self.CmdList, cmd)
}

func (self *Support) protocolMatches(msg *p.Message) bool {
	pv := int(msg.GetProduct())
	return pv >= self.MinProtocol && pv <= self.MaxProtocol
}

func (self *Support) SupportBiz(biz int) bool {
	return self.BizList == nil || inArray(self.BizList, biz)
}

func inArray(arr []int, target int) bool {
	for _, v := range arr {
		if v == target {
			return true
		}
	}

	return false
}
