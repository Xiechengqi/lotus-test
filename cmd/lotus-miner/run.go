package main

import (
	"fmt"
	"github.com/filecoin-project/lotus/octopus"
	_ "net/http/pprof"
	"os"

	"github.com/filecoin-project/lotus/api/v1api"

	"github.com/filecoin-project/lotus/api/v0api"

	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/ulimit"
	"github.com/filecoin-project/lotus/metrics"
	"github.com/filecoin-project/lotus/node"
	"github.com/filecoin-project/lotus/node/config"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/repo"
)

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start a lotus miner process",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "miner-api",
			Usage: "2345",
		},
		&cli.BoolFlag{
			Name:  "enable-gpu-proving",
			Usage: "enable use of GPU for mining operations",
			Value: true,
		},
		&cli.BoolFlag{
			Name:  "nosync",
			Usage: "don't check full-node sync status",
		},
		&cli.BoolFlag{
			Name:  "manage-fdlimit",
			Usage: "manage open file limit",
			Value: true,
		},
		&cli.StringFlag{
			Name:  "redis-conn",
			Usage: "redis connection string. if in cluster mode, separated each one with comma",
			Value: "localhost:6379",
		},
		&cli.StringFlag{
			Name:  "redis-password",
			Usage: "redis connection password",
			Value: "",
		},
		&cli.BoolFlag{
			Name:  "enable-winning-post",
			Usage: "support doing winning post",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "enable-window-post",
			Usage: "support doing window post",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "disable-sealing",
			Usage: "disable sealing",
			Value: false,
		},
		&cli.StringFlag{
			Name:  "ability",
			Usage: "sealing task abilities",
			Value: "PC1:1,PC2:1,C2:1",
		},
	},
	Action: func(cctx *cli.Context) error {
		if !cctx.Bool("enable-gpu-proving") {
			err := os.Setenv("BELLMAN_NO_GPU", "true")
			if err != nil {
				return err
			}
		}

		if cctx.Bool("enable-window-post") {
			os.Setenv("ENABLE_WINDOW_POST", "true")
		} else {
			os.Unsetenv("ENABLE_WINDOW_POST")
		}

		if cctx.Bool("enable-winning-post") {
			os.Setenv("ENABLE_WINNING_POST", "true")
		} else {
			os.Unsetenv("ENABLE_WINNING_POST")
		}
		if cctx.Bool("disable-sealing") {
			os.Setenv("DISABLE_SEALING", "true")
		} else {
			os.Unsetenv("DISABLE_SEALING")
		}

		ability := cctx.String("ability")
		if ability != "" {
			os.Setenv("ABILITY", ability)
		}

		//redisConn := cctx.String("redis-conn")
		//if redisConn == "" {
		//	log.Errorf("redis conn must be set")
		//	return xerrors.Errorf("redis-conn must be set.")
		//}
		//os.Setenv("REDIS_CONN", redisConn)
		if os.Getenv("REDIS_CONN") == "" {
			log.Errorf("env REDIS_CONN must be set.")
			return nil
		}
		//
		//redisPassword := cctx.String("redis-password")
		//os.Setenv("REDIS_PASSWORD", redisPassword)
		//if os.Getenv("REDIS_PASSWORD") == "" {
		//	log.Errorf("env REDIS_PASSWORD must be set.")
		//	return nil
		//}

		ctx, _ := tag.New(lcli.DaemonContext(cctx),
			tag.Insert(metrics.Version, build.BuildVersion),
			tag.Insert(metrics.Commit, build.CurrentCommit),
			tag.Insert(metrics.NodeType, "miner"),
		)
		// Register all metric views
		if err := view.Register(
			metrics.MinerNodeViews...,
		); err != nil {
			log.Fatalf("Cannot register the view: %v", err)
		}
		// Set the metric to one so it is published to the exporter
		stats.Record(ctx, metrics.LotusInfo.M(1))

		if err := checkV1ApiSupport(ctx, cctx); err != nil {
			return err
		}

		nodeApi, ncloser, ainfo, err := lcli.GetFullNodeAPIV1More(cctx)
		if err != nil {
			return xerrors.Errorf("getting full node api: %w", err)
		}
		//defer ncloser()
		nodepool := octopus.NewFullNodePool(cctx, ctx, string(ainfo.Token)+":"+ainfo.Addr, nodeApi, ncloser)
		defer nodepool.Close()

		v, err := nodeApi.Version(ctx)
		if err != nil {
			return err
		}

		if cctx.Bool("manage-fdlimit") {
			if _, _, err := ulimit.ManageFdLimit(); err != nil {
				log.Errorf("setting file descriptor limit: %s", err)
			}
		}

		if v.APIVersion != api.FullAPIVersion1 {
			return xerrors.Errorf("lotus-daemon API version doesn't match: expected: %s", api.APIVersion{APIVersion: api.FullAPIVersion1})
		}

		log.Info("Checking full node sync status")

		if !cctx.Bool("nosync") {
			if err := lcli.SyncWait(ctx, &v0api.WrapperV1Full{FullNode: nodeApi}, false); err != nil {
				return xerrors.Errorf("sync wait: %w", err)
			}
		}

		minerRepoPath := cctx.String(FlagMinerRepo)
		r, err := repo.NewFS(minerRepoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			return xerrors.Errorf("repo at '%s' is not initialized, run 'lotus-miner init' to set it up", minerRepoPath)
		}

		lr, err := r.Lock(repo.StorageMiner)
		if err != nil {
			return err
		}
		c, err := lr.Config()
		if err != nil {
			return err
		}
		cfg, ok := c.(*config.StorageMiner)
		if !ok {
			return xerrors.Errorf("invalid config for repo, got: %T", c)
		}

		bootstrapLibP2P := cfg.Subsystems.EnableMarkets

		err = lr.Close()
		if err != nil {
			return err
		}

		shutdownChan := make(chan struct{})

		var minerapi api.StorageMiner
		stop, err := node.New(ctx,
			node.StorageMiner(&minerapi, cfg.Subsystems),
			node.Override(new(dtypes.ShutdownChan), shutdownChan),
			node.Base(),
			node.Repo(r),

			node.ApplyIf(func(s *node.Settings) bool { return cctx.IsSet("miner-api") },
				node.Override(new(dtypes.APIEndpoint), func() (dtypes.APIEndpoint, error) {
					return multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/" + cctx.String("miner-api"))
				})),
			node.Override(new(v1api.FullNode), nodeApi),
			node.Override(new(*octopus.FullNodePool), nodepool),
		)
		if err != nil {
			return xerrors.Errorf("creating node: %w", err)
		}

		endpoint, err := r.APIEndpoint()
		if err != nil {
			return xerrors.Errorf("getting API endpoint: %w", err)
		}

		if bootstrapLibP2P {
			log.Infof("Bootstrapping libp2p network with full node")

			// Bootstrap with full node
			remoteAddrs, err := nodeApi.NetAddrsListen(ctx)
			if err != nil {
				return xerrors.Errorf("getting full node libp2p address: %w", err)
			}

			if err := minerapi.NetConnect(ctx, remoteAddrs); err != nil {
				return xerrors.Errorf("connecting to full node (libp2p): %w", err)
			}
		}

		log.Infof("Remote version %s", v)

		// Instantiate the miner node handler.
		handler, err := node.MinerHandler(minerapi, true)
		if err != nil {
			return xerrors.Errorf("failed to instantiate rpc handler: %w", err)
		}

		// Serve the RPC.
		rpcStopper, err := node.ServeRPC(handler, "lotus-miner", endpoint)
		if err != nil {
			return fmt.Errorf("failed to start json-rpc endpoint: %s", err)
		}

		// Monitor for shutdown.
		finishCh := node.MonitorShutdown(shutdownChan,
			node.ShutdownHandler{Component: "rpc server", StopFunc: rpcStopper},
			node.ShutdownHandler{Component: "miner", StopFunc: stop},
		)

		<-finishCh
		return nil
	},
}
