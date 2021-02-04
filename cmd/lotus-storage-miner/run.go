package main

import (
	"context"
	"encoding/json"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
        scServer "github.com/fileguard/sector-counter/server"

	mux "github.com/gorilla/mux"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/apistruct"
	"github.com/filecoin-project/lotus/build"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/ulimit"
	"github.com/filecoin-project/lotus/metrics"
	"github.com/filecoin-project/lotus/node"
	"github.com/filecoin-project/lotus/node/impl"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/repo"
)

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start a lotus miner process",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "window-post",
			Usage: "enable window PoSt",
			Value: true,
		},
		&cli.BoolFlag{
			Name:  "winning-post",
			Usage: "enable winning PoSt",
			Value: true,
		},
		&cli.BoolFlag{
			Name:  "p2p",
			Usage: "enable P2P",
			Value: true,
		},
		&cli.StringFlag{
			Name:  "sctype",
			Usage: "sector counter type(alloce,get)",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "sclisten",
			Usage: "host address and port the sector counter will listen on",
			Value: "",
		},
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
	},
	Action: func(cctx *cli.Context) error {
		if cctx.Bool("window-post") {
			os.Setenv("LOTUS_WINDOW_POST", "true")
		} else {
			os.Unsetenv("LOTUS_WINDOW_POST")
		}

		if cctx.Bool("winning-post") {
			os.Setenv("LOTUS_WINNING_POST", "true")
		} else {
			os.Unsetenv("LOTUS_WINNING_POST")
		}

		scType := cctx.String("sctype")
		if scType == "alloce" || scType == "get" {
			os.Setenv("SC_TYPE", scType)

			scListen := cctx.String("sclisten")
			if scListen == "" {
				log.Errorf("sclisten must be set")
				return nil
			}
			os.Setenv("SC_LISTEN", scListen)

			if scType == "alloce" {
				scFilePath := filepath.Join(cctx.String(FlagMinerRepo), "sectorid")
				go scServer.Run(scFilePath)
			}
		} else {
			os.Unsetenv("SC_TYPE")
		}

		if !cctx.Bool("enable-gpu-proving") {
			err := os.Setenv("BELLMAN_NO_GPU", "true")
			if err != nil {
				return err
			}
		}

		nodeApi, ncloser, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return xerrors.Errorf("getting full node api: %w", err)
		}
		defer ncloser()
		ctx := lcli.DaemonContext(cctx)

		// Register all metric views
		if err := view.Register(
			metrics.DefaultViews...,
		); err != nil {
			log.Fatalf("Cannot register the view: %v", err)
		}

		v, err := nodeApi.Version(ctx)
		if err != nil {
			return err
		}

		if cctx.Bool("manage-fdlimit") {
			if _, _, err := ulimit.ManageFdLimit(); err != nil {
				log.Errorf("setting file descriptor limit: %s", err)
			}
		}

		if v.APIVersion != build.FullAPIVersion {
			return xerrors.Errorf("lotus-daemon API version doesn't match: expected: %s", api.Version{APIVersion: build.FullAPIVersion})
		}

		log.Info("Checking full node sync status")

		if !cctx.Bool("nosync") {
			if err := lcli.SyncWait(ctx, nodeApi, false); err != nil {
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

		hostname, err := os.Hostname()
		if err != nil {
			hostname = ""
		}
		fileDst := filepath.Join(minerRepoPath, "myscheduler.json")
		_, errorFile := os.Stat(fileDst)
		if os.IsNotExist(errorFile) {
			//persisting myScheduler metadata start//
			b, err := json.MarshalIndent(&stores.MySchedulerMeta{
				WorkerName:         hostname,
				AddPieceMax:        uint64(0),
				PreCommit1Max:      uint64(0),
				PreCommit2Max:      uint64(0),
				Commit2Max:         uint64(0),
				DiskHoldMax:        uint64(0),
				APDiskHoldMax:      uint64(0),
				ForceP1FromLocalAP: true,
				ForceP2FromLocalP1: true,
				ForceC2FromLocalP2: false,
				IsPlanOffline:      false,
				AllowP2C2Parallel:  false,
				AutoPledgeDiff:     uint64(0),
			}, "", "  ")
			if err != nil {
				//return xerrors.Errorf("marshaling myScheduler config: %w", err)
				log.Error("marshaling myScheduler config:", err)
			}
			if err := ioutil.WriteFile(filepath.Join(minerRepoPath, "myscheduler.json"), b, 0644); err != nil {
				//return xerrors.Errorf("persisting myScheduler metadata (%s): %w", filepath.Join(minerRepoPath, "myscheduler.json"), err)
				log.Error("persisting myScheduler metadata:", err)
			}
			//persisting myScheduler metadata end//
		}

		fileDst = filepath.Join(minerRepoPath, "testOpenSource.json")
		_, errorFile = os.Stat(fileDst)
		if os.IsNotExist(errorFile) {
			//persisting TestSchedulerMeta metadata start//
			b, err := json.MarshalIndent(&stores.TestSchedulerMeta{
				AddPieceMax:        uint64(1),
				PreCommit1Max:      uint64(1),
				PreCommit2Max:      uint64(1),
				Commit2Max:         uint64(1),
				ForceP1FromLocalAP: true,
				ForceP2FromLocalP1: true,
				ForceC2FromLocalP2: false,
				AllowP2C2Parallel:  true,
			}, "", "  ")
			if err != nil {
				//return xerrors.Errorf("marshaling TestSchedulerMeta config: %w", err)
				log.Error("marshaling testOpenSource config:", err)
			}
			if err := ioutil.WriteFile(filepath.Join(minerRepoPath, "testOpenSource.json"), b, 0644); err != nil {
				//return xerrors.Errorf("persisting testOpenSource metadata (%s): %w", filepath.Join(minerRepoPath, "testOpenSource.json"), err)
				log.Error("persisting testOpenSource metadata:", err)
			}
			//persisting TestSchedulerMeta metadata end//
		}

		shutdownChan := make(chan struct{})

		var minerapi api.StorageMiner
		stop, err := node.New(ctx,
			node.StorageMiner(&minerapi),
			node.Override(new(dtypes.ShutdownChan), shutdownChan),
			node.Online(),
			node.Repo(r),

			node.ApplyIf(func(s *node.Settings) bool { return cctx.IsSet("miner-api") },
				node.Override(new(dtypes.APIEndpoint), func() (dtypes.APIEndpoint, error) {
					return multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/" + cctx.String("miner-api"))
				})),
			node.Override(new(api.FullNode), nodeApi),
		)
		if err != nil {
			return xerrors.Errorf("creating node: %w", err)
		}

		endpoint, err := r.APIEndpoint()
		if err != nil {
			return xerrors.Errorf("getting API endpoint: %w", err)
		}

		if cctx.Bool("p2p") {
			// Bootstrap with full node
			remoteAddrs, err := nodeApi.NetAddrsListen(ctx)
			if err != nil {
				return xerrors.Errorf("getting full node libp2p address: %w", err)
			}
	
			if err := minerapi.NetConnect(ctx, remoteAddrs); err != nil {
				return xerrors.Errorf("connecting to full node (libp2p): %w", err)
			}
		} else {
			log.Warn("This miner will be disable p2p")
		}

		log.Infof("Remote version %s", v)

		lst, err := manet.Listen(endpoint)
		if err != nil {
			return xerrors.Errorf("could not listen: %w", err)
		}

		mux := mux.NewRouter()

		rpcServer := jsonrpc.NewServer()
		rpcServer.Register("Filecoin", apistruct.PermissionedStorMinerAPI(metrics.MetricedStorMinerAPI(minerapi)))

		mux.Handle("/rpc/v0", rpcServer)
		mux.PathPrefix("/remote").HandlerFunc(minerapi.(*impl.StorageMinerAPI).ServeRemote)
		mux.PathPrefix("/").Handler(http.DefaultServeMux) // pprof

		ah := &auth.Handler{
			Verify: minerapi.AuthVerify,
			Next:   mux.ServeHTTP,
		}

		srv := &http.Server{
			Handler: ah,
			BaseContext: func(listener net.Listener) context.Context {
				ctx, _ := tag.New(context.Background(), tag.Upsert(metrics.APIInterface, "lotus-miner"))
				return ctx
			},
		}

		sigChan := make(chan os.Signal, 2)
		go func() {
			select {
			case sig := <-sigChan:
				log.Warnw("received shutdown", "signal", sig)
			case <-shutdownChan:
				log.Warn("received shutdown")
			}

			log.Warn("Shutting down...")
			if err := stop(context.TODO()); err != nil {
				log.Errorf("graceful shutting down failed: %s", err)
			}
			if err := srv.Shutdown(context.TODO()); err != nil {
				log.Errorf("shutting down RPC server failed: %s", err)
			}
			log.Warn("Graceful shutdown successful")
		}()
		signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

		return srv.Serve(manet.NetListener(lst))
	},
}
