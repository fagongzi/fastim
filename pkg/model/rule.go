package model

import (
	"encoding/json"
)

type Rule struct {
}

func UnMarshalRule(data []byte) *Rule {
	v := &Rule{}
	json.Unmarshal(data, v)

	return v
}

func (self *Rule) Marshal() []byte {
	v, _ := json.Marshal(self)
	return v
}
