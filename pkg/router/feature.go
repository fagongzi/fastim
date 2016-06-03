package router

const (
	FEATURE_OPEN_ENCRYPT = 1 << iota // open encrypt
)

type Feature struct {
	mask int64
}

func NewFeature(mask int64) *Feature {
	return &Feature{mask: mask}
}

func (self *Feature) Encrypt() {
	self.mask |= FEATURE_OPEN_ENCRYPT
}

func (self *Feature) ActiveEncrypt() bool {
	return self.mask&FEATURE_OPEN_ENCRYPT != 0
}
