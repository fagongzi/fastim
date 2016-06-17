package util

import (
	"github.com/CodisLabs/codis/pkg/utils/atomic2"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"time"
)

type point struct {
	count atomic2.Int64
	qps   int
}

func (self *point) dump(target *point) {
	target.count.Set(self.count.Get())
}

type Analysis struct {
	points         map[string]*point
	recentlyPoints map[string]map[int]*Recently
}

type Recently struct {
	period   int64
	prev     *point
	current  *point
	dumpCurr bool
	qps      int
	count    int64
}

func newRecently(period int64) *Recently {
	return &Recently{
		prev:    newPoint(),
		current: newPoint(),
		period:  period,
	}
}

func newPoint() *point {
	return &point{}
}

func NewAnalysis() *Analysis {
	return &Analysis{
		points:         make(map[string]*point),
		recentlyPoints: make(map[string]map[int]*Recently),
	}
}

func (self *Recently) record(p *point) {
	if self.dumpCurr {
		p.dump(self.current)
		self.calc()
	} else {
		p.dump(self.prev)
	}

	self.dumpCurr = !self.dumpCurr
}

func (self *Recently) calc() {
	self.count = self.current.count.Get() - self.prev.count.Get()

	if self.count < 0 {
		self.count = 0
	}

	if self.count > self.count {
		self.qps = int(self.count / self.period)
	} else {
		self.qps = int(self.count / self.period)
	}
}

func (self *Analysis) AddRecent(key string, secs int) {
	_, ok := self.recentlyPoints[key][secs]
	if ok {
		log.Infof("Analysis already <%s,%d> added", key, secs)
		return
	}

	recently := newRecently(int64(secs))
	self.recentlyPoints[key][secs] = recently
	timer := time.NewTicker(time.Duration(secs) * time.Second)

	go func() {
		for {
			// TODO: remove
			<-timer.C

			p, ok := self.points[key]

			if ok {
				recently.record(p)
			}
		}
	}()

	log.Infof("Analysis <%s,%d> added", key, secs)
}

func (self *Analysis) addNewAnalysis(key string) {
	self.points[key] = &point{}
	self.recentlyPoints[key] = make(map[int]*Recently)
}

func (self *Analysis) GetRecentlyCount(server string, secs int) int {
	points, ok := self.recentlyPoints[server]

	if !ok {
		return 0
	}

	point, ok := points[secs]

	if !ok {
		return 0
	}

	return int(point.count)
}

func (self *Analysis) GetQPS(server string, secs int) int {
	points, ok := self.recentlyPoints[server]

	if !ok {
		return 0
	}

	point, ok := points[secs]

	if !ok {
		return 0
	}

	return int(point.qps)
}

func (self *Analysis) Request(key string) {
	p := self.points[key]
	p.count.Incr()
}
