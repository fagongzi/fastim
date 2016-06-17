package main

import (
	"flag"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/fagongzi/fastim/pkg/bind"
	"github.com/fagongzi/fastim/pkg/conf"
	l "github.com/fagongzi/fastim/pkg/log"
	"github.com/fagongzi/fastim/pkg/model"
	p "github.com/fagongzi/fastim/pkg/protocol"
	"github.com/fagongzi/fastim/pkg/registor"
	"github.com/fagongzi/fastim/pkg/service"
	s "github.com/fagongzi/fastim/pkg/service_session"
	"github.com/fagongzi/fastim/pkg/util"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var (
	cpus       = flag.Int("cpus", 1, "use cpu nums")
	addr       = flag.String("addr", ":443", "listen addr of tcp protocol.(e.g. ip:port)")
	etcdAddr   = flag.String("etcd-addr", "http://127.0.0.1:2379", "etcd address, use ',' to splite.")
	etcdPrefix = flag.String("etcd-prefix", "/fastim", "etcd node prefix.")
	redisAddr  = flag.String("redis-addr", "127.0.0.1:2379", "redis address, use ',' to splite.")
)

var (
	maxIdle = flag.Int("max-idle", 10, "max num that redis connection idle")
)

var (
	supportProduct     = flag.Int("product", 0, "support product")
	supportBizs        = flag.String("bizs", "", "support bizs, use ',' to splite")
	supportCmds        = flag.String("cmds", "", "support cmds, use ',' to splite")
	supportMinProtocol = flag.Int("min-protocol", -1, "support min protocol version")
	supportMaxProtocol = flag.Int("max-protocol", -1, "support min protocol version")

	writeBufQueueSize = flag.Int("buf-write-queue", 1024, "write queue buf size")
	delaySessionClose = flag.Int("delay-session-close", 60*10, "received sessin closed from router, delay seconds to process")
)

var (
	registorTTL    = flag.Uint64("registor-ttl", 10, "registor to etcd's ttl, unit: second.")
	timeoutRead    = flag.Duration("timeout-read", time.Minute*5, "read timeout.")
	timeoutConnect = flag.Duration("timeout-connect", time.Second*5, "connect timeout.")
	timeoutWrite   = flag.Duration("timeout-write", time.Second*30, "write timeout.")
	timeoutIdle    = flag.Duration("timeout-idle", time.Hour, "timeout that how long redis connection idle.")
)

var (
	logFile  = flag.String("log-file", "", "which file to record log, if not set stdout to use.")
	logLevel = flag.String("log-level", "info", "log level.")
)

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*cpus)

	l.InitLog(*logFile)
	l.SetLogLevel(*logLevel)

	util.Init()

	bizs, err := getIntArr(*supportBizs)
	if err != nil {
		log.PanicErrorf(err, "support-bizs<%s> input error.", *supportBizs)
	}

	cmds, err := getIntArr(*supportCmds)
	if err != nil {
		log.PanicErrorf(err, "support-cmds<%s> input error.", *supportCmds)
	}

	support := &model.Support{
		Addr:        util.ConvertToIp(*addr),
		Product:     *supportProduct,
		CmdList:     cmds,
		BizList:     bizs,
		MinProtocol: *supportMinProtocol,
		MaxProtocol: *supportMaxProtocol,
	}

	cnf := &conf.ServiceConf{
		Etcd: &conf.EtcdConf{
			EtcdAddrs:   strings.Split(*etcdAddr, ","),
			EtcdPrefix:  *etcdPrefix,
			RegisterTTL: *registorTTL,
		},
		Redis: &conf.RedisConf{
			RedisAddrs:  strings.Split(*redisAddr, ","),
			TimeoutIdle: *timeoutIdle,
			MaxIdle:     *maxIdle,
		},
		Timeout: &conf.TimeoutConf{
			TimeoutRead:    *timeoutRead,
			TimeoutConnect: *timeoutConnect,
			TimeoutWrite:   *timeoutWrite,
		},

		Addr:              *addr,
		WriteBufQueueSize: *writeBufQueueSize,
		DelaySessionClose: *delaySessionClose,
	}

	log.Infof("%s start with support <%+v>", util.MODULE_SERVICE, support)

	r := registor.NewEtcdRegistor(cnf.Etcd.EtcdAddrs, cnf.Etcd.EtcdPrefix)

	redisPool := util.NewRedisPool(cnf.Redis.RedisAddrs, cnf.Redis.MaxIdle, cnf.Redis.TimeoutIdle)
	routing, err := bind.NewRedisRouting(cnf.Addr, redisPool, r, false)
	if err != nil {
		log.WarnErrorf(err, "%s runtime failure", util.MODULE_SERVICE)
	}

	syncProtocol := p.NewRedisSyncProtocol(redisPool)
	server := service.NewServer(cnf, support, routing, r, syncProtocol)
	service := s.NewSessionService(cnf, server)

	err = service.Serve()
	if err != nil {
		log.PanicError(err, "service start failure.")
	}
}

func getIntArr(value string) ([]int, error) {
	values := strings.Split(value, ",")

	intValues := make([]int, len(values))

	for index, v := range values {
		intValue, err := strconv.Atoi(v)
		if err != nil {
			return nil, err
		}

		intValues[index] = intValue
	}

	return intValues, nil
}
