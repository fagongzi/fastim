package main

import (
	"flag"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/fagongzi/fastim/pkg/bind"
	l "github.com/fagongzi/fastim/pkg/log"
	"github.com/fagongzi/fastim/pkg/registor"
	"github.com/fagongzi/fastim/pkg/router"
	"github.com/fagongzi/fastim/pkg/util"
	"runtime"
	"strings"
	"time"
)

var (
	cpus         = flag.Int("cpus", 1, "use cpu nums")
	addr         = flag.String("addr", ":443", "listen msg from client.(e.g. ip:port)")
	internalAddr = flag.String("internal-addr", ":9001", "listen msg from backend.(e.g. ip:port)")
	etcdAddr     = flag.String("etcd-addr", "http://127.0.0.1:2379", "etcd address, use ',' to splite.")
	etcdPrefix   = flag.String("etcd-prefix", "/fastim", "etcd node prefix.")
	redisAddr    = flag.String("redis-addr", "127.0.0.1:2379", "redis address, use ',' to splite.")
)

var (
	maxIdle = flag.Int("max-idle", 10, "max num that redis connection idle")
)

var (
	registorTTL            = flag.Uint64("registor-ttl", 10, "registor to etcd's ttl, unit: second.")
	timeoutConnectBackend  = flag.Duration("timeout-connect-to-backend", time.Second*5, "timeout that how long to connect to backend.")
	timeoutReadFromBackend = flag.Duration("timeout-read-from-backend", time.Second*30, "timeout that how long read from backend.")
	timeoutWriteToBackend  = flag.Duration("timeout-write-to-backend", time.Second*30, "timeout that how long write to backend.")
	timeoutIdle            = flag.Duration("timeout-idle", time.Hour, "timeout that how long redis connection idle.")
)

var (
	enableEncrypt = flag.Bool("encrypt", false, "enable packet encrypt")
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

	cnf := &router.Conf{
		Addr:         *addr,
		InternalAddr: *internalAddr,
		EtcdAddrs:    strings.Split(*etcdAddr, ","),
		RedisAddrs:   strings.Split(*redisAddr, ","),
		EtcdPrefix:   *etcdPrefix,
		RegisterTTL:  *registorTTL,

		TimeoutConnectBackend:  *timeoutConnectBackend,
		TimeoutReadFromBackend: *timeoutReadFromBackend,
		TimeoutWriteToBackend:  *timeoutWriteToBackend,
		TimeoutIdle:            *timeoutIdle,

		MaxIdle: *maxIdle,

		EnableEncrypt: *enableEncrypt,
	}

	log.Infof("%s start with conf <%+v>", util.MODULE_FRONTEND_TCP, cnf)

	registor := registor.NewEtcdRegistor(cnf.EtcdAddrs, cnf.EtcdPrefix)
	redisPool := bind.NewRedisPool(cnf.RedisAddrs, cnf.MaxIdle, cnf.TimeoutIdle)
	routing, err := bind.NewRedisRouting(util.ConvertToIp(cnf.InternalAddr), redisPool, registor, true)
	if err != nil {
		log.WarnErrorf(err, "%s runtime failure", util.MODULE_FRONTEND_TCP)
	}

	go routing.Watch()

	service := router.NewService(cnf, routing)
	svr := router.NewTCPServer(cnf, registor, service, router.NewBackends(cnf, service))
	err = svr.Serve()
	if err != nil {
		log.PanicErrorf(err, "%s runtime failure", util.MODULE_FRONTEND_TCP)
	}
}
