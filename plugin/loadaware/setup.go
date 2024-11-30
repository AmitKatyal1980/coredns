package loadaware

import (
	"strconv"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
	clog "github.com/coredns/coredns/plugin/pkg/log"
	"github.com/redis/go-redis/v9"
)

const (
	loadAware = "loadaware"
)

var log = clog.NewWithPlugin(loadAware)

func init() { plugin.Register(loadAware, setup) }

type lcFuncs struct {
	redisAddr      string
	redisKey       string
	loadThreshold  int
	onStartUpFunc  func() error
	onShutdownFunc func() error
}

func setup(c *caddy.Controller) error {
	lc, err := parse(c)
	if err != nil {
		return plugin.Error("loadAware", err)
	}

	// Log when the plugin setup is happening
	log.Infof("Registering LoadAware plugin with Redis address %s", lc.redisAddr)

	// Register the plugin in the plugin chain
	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		log.Info("LoadAware plugin has been added to the chain.")
		return &LoadAware{Next: next, RedisClient: buildClient(lc.redisAddr),
			RedisKey: lc.redisKey, LoadThreshold: lc.loadThreshold}
	})

	// Register startup and shutdown hooks if needed
	if lc.onStartUpFunc != nil {
		c.OnStartup(lc.onStartUpFunc)
	}
	if lc.onShutdownFunc != nil {
		c.OnShutdown(lc.onShutdownFunc)
	}

	return nil
}

func parse(c *caddy.Controller) (*lcFuncs, error) {
	var redisAddress string
	var redisKey string
	var loadThreshold int
	var funcs lcFuncs

	for c.Next() {
		args := c.RemainingArgs()
		if len(args) == 0 {
			continue
		}
		// parse Redis and load configuration
		if len(args) == 3 {
			redisAddress = args[0]
			redisKey = args[1]
			loadThreshold, _ = strconv.Atoi(args[2])
			funcs = lcFuncs{
				redisAddr:     redisAddress,
				redisKey:      redisKey,
				loadThreshold: loadThreshold,
				onStartUpFunc: func() error {
					log.Infof("onStartUpFunc: Connecting to Redis at %s with key %s", redisAddress, redisKey)
					return nil
				},
				onShutdownFunc: func() error {
					log.Infof("Shutting down Redis connection")
					// Close Redis connection here if needed
					return nil
				},
			}
		} else {
			return nil, c.ArgErr()
		}
	}

	return &funcs, nil
}

func buildClient(url string) *redis.Client {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil
	}
	return redis.NewClient(opt)
}
