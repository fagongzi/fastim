package main

import (
	"flag"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/fagongzi/fastim/pkg/bind"
	l "github.com/fagongzi/fastim/pkg/log"
	"github.com/fagongzi/fastim/pkg/model"
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
)

var (
	registorTTL           = flag.Uint64("registor-ttl", 10, "registor to etcd's ttl, unit: second.")
	timeoutConnectRouter  = flag.Duration("timeout-connect-to-router", time.Second*5, "timeout that how long to connect to backend.")
	timeoutReadFromRouter = flag.Duration("timeout-read-from-router", time.Second*30, "timeout that how long read from backend.")
	timeoutWriteToRouter  = flag.Duration("timeout-write-to-router", time.Second*30, "timeout that how long write to backend.")
	timeoutIdle           = flag.Duration("timeout-idle", time.Hour, "timeout that how long redis connection idle.")
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

	conf := &service.Conf{
		Addr:        *addr,
		EtcdAddrs:   strings.Split(*etcdAddr, ","),
		RedisAddrs:  strings.Split(*redisAddr, ","),
		EtcdPrefix:  *etcdPrefix,
		RegisterTTL: *registorTTL,

		TimeoutConnectRouter:  *timeoutConnectRouter,
		TimeoutReadFromRouter: *timeoutReadFromRouter,
		TimeoutWriteToRouter:  *timeoutWriteToRouter,
		TimeoutIdle:           *timeoutIdle,

		MaxIdle: *maxIdle,
	}

	log.Infof("%s start with support <%+v>", util.MODULE_SERVICE, support)

	r := registor.NewEtcdRegistor(conf.EtcdAddrs, conf.EtcdPrefix)

	redisPool := bind.NewRedisPool(conf.RedisAddrs, conf.MaxIdle, conf.TimeoutIdle)
	routing, err := bind.NewRedisRouting(conf.Addr, redisPool, r, false)
	if err != nil {
		log.WarnErrorf(err, "%s runtime failure", util.MODULE_SERVICE)
	}

	service := s.NewSessionService(conf, support, routing, r)

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
