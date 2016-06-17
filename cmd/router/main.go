package main

import (
	"flag"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/fagongzi/fastim/pkg/bind"
	"github.com/fagongzi/fastim/pkg/conf"
	l "github.com/fagongzi/fastim/pkg/log"
	"github.com/fagongzi/fastim/pkg/registor"
	"github.com/fagongzi/fastim/pkg/router"
	"github.com/fagongzi/fastim/pkg/util"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"
)

var (
	cpus         = flag.Int("cpus", 1, "use cpu nums")
	addr         = flag.String("addr", ":443", "listen msg from client.(e.g. ip:port)")
	internalAddr = flag.String("internal-addr", ":9001", "listen msg from backend.(e.g. ip:port)")
	etcdAddr     = flag.String("etcd-addr", "http://127.0.0.1:2379", "etcd address, use ',' to splite.")
	etcdPrefix   = flag.String("etcd-prefix", "/fastim", "etcd node prefix.")
	redisAddr    = flag.String("redis-addr", "127.0.0.1:2379", "redis address, use ',' to splite.")
	bucketSize   = flag.Int("bucket-size", 512, "how many buckets which used for store session are used in router")
	maxRetry     = flag.Int("max-retry", 3, "how many times retry router to send session closed notify to backend")
)

var (
	maxIdle = flag.Int("max-idle", 10, "max num that redis connection idle")
)

var (
	registorTTL    = flag.Uint64("registor-ttl", 10, "registor to etcd's ttl, unit: second.")
	timeoutRead    = flag.Duration("timeout-read", time.Minute*5, "read timeout.")
	timeoutConnect = flag.Duration("timeout-connect", time.Second*5, "connect timeout.")
	timeoutWrite   = flag.Duration("timeout-write", time.Second*30, "write timeout.")
	timeoutIdle    = flag.Duration("timeout-idle", time.Hour, "timeout that how long redis connection idle.")

	timeoutWaitAck = flag.Duration("timeout-wait-ack", time.Second*30, "timeout that wait for sesison closed ack.")
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

	cnf := &conf.RouterConf{
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

		Addr:           *addr,
		InternalAddr:   *internalAddr,
		BucketSize:     *bucketSize,
		MaxRetry:       *maxRetry,
		TimeoutWaitAck: *timeoutWaitAck,

		EnableEncrypt: *enableEncrypt,
	}

	log.Infof("%s start with conf <%+v>", util.MODULE_FRONTEND_TCP, cnf)

	registor := registor.NewEtcdRegistor(cnf.Etcd.EtcdAddrs, cnf.Etcd.EtcdPrefix)
	redisPool := util.NewRedisPool(cnf.Redis.RedisAddrs, cnf.Redis.MaxIdle, cnf.Redis.TimeoutIdle)
	routing, err := bind.NewRedisRouting(util.ConvertToIp(cnf.InternalAddr), redisPool, registor, true)
	if err != nil {
		log.WarnErrorf(err, "%s runtime failure", util.MODULE_FRONTEND_TCP)
	}

	service := router.NewService(cnf, routing, registor)
	svr := router.NewTCPServer(cnf, service)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, os.Kill)

	go func() {
		<-c
		log.Infof("ctrl-c or SIGTERM found, router will exit")
		svr.Stop()
	}()

	err = svr.Serve()
	if err != nil {
		log.PanicErrorf(err, "%s runtime failure", util.MODULE_FRONTEND_TCP)
	} else {
		log.Infof("Router is Exit.")
	}
}
