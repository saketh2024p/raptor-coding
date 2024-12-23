// Package ipfsproxy implements the Cluster API interface by providing an
// IPFS HTTP interface as exposed by the go-ipfs daemon.
//
// In this API, select endpoints like pin*, add*, and repo* endpoints are used
// to instead perform cluster operations. Requests for any other endpoints are
// passed to the underlying IPFS daemon.
package ipfsproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/adder/adderutils"
	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs-cluster/ipfs-cluster/rpcutil"

	handlers "github.com/gorilla/handlers"
	mux "github.com/gorilla/mux"
	cid "github.com/ipfs/go-cid"
	cmd "github.com/ipfs/go-ipfs-cmds"
	logging "github.com/ipfs/go-log/v2"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	peer "github.com/libp2p/go-libp2p/core/peer"
	madns "github.com/multiformats/go-multiaddr-dns"
	manet "github.com/multiformats/go-multiaddr/net"

	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/plugin/ochttp/propagation/tracecontext"
	"go.opencensus.io/trace"
)

// DNSTimeout is used when resolving DNS multiaddresses in this module
var DNSTimeout = 5 * time.Second

var (
	logger      = logging.Logger("ipfsproxy")
	proxyLogger = logging.Logger("ipfsproxylog")
)

// Server offers an IPFS API, hijacking some interesting requests
// and forwarding the rest to the ipfs daemon
// it proxies HTTP requests to the configured IPFS
// daemon. It is able to intercept these requests though, and
// perform extra operations on them.
type Server struct {
	ctx    context.Context
	cancel func()

	config     *Config
	nodeScheme string
	nodeAddr   string

	rpcClient *rpc.Client
	rpcReady  chan struct{}

	listeners    []net.Listener         // proxy listener
	server       *http.Server           // proxy server
	reverseProxy *httputil.ReverseProxy // allows to talk to IPFS

	ipfsHeadersStore sync.Map

	shutdownLock sync.Mutex
	shutdown     bool
	wg           sync.WaitGroup
}

type ipfsPinType struct {
	Type string
}

type ipfsPinLsResp struct {
	Keys map[string]ipfsPinType
}

type ipfsPinOpResp struct {
	Pins []string
}

// From https://github.com/ipfs/go-ipfs/blob/master/core/coreunix/add.go#L49
type ipfsAddResp struct {
	Name  string
	Hash  string `json:",omitempty"`
	Bytes int64  `json:",omitempty"`
	Size  string `json:",omitempty"`
}

type logWriter struct {
}

func (lw logWriter) Write(b []byte) (int, error) {
	proxyLogger.Infof(string(b))
	return len(b), nil
}

// New returns and ipfs Proxy component
func New(cfg *Config) (*Server, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	nodeMAddr := cfg.NodeAddr
	// dns multiaddresses need to be resolved first
	if madns.Matches(nodeMAddr) {
		ctx, cancel := context.WithTimeout(context.Background(), DNSTimeout)
		defer cancel()
		resolvedAddrs, err := madns.Resolve(ctx, cfg.NodeAddr)
		if err != nil {
			logger.Error(err)
			return nil, err
		}
		nodeMAddr = resolvedAddrs[0]
	}

	_, nodeAddr, err := manet.DialArgs(nodeMAddr)
	if err != nil {
		return nil, err
	}

	var listeners []net.Listener
	for _, addr := range cfg.ListenAddr {
		proxyNet, proxyAddr, err := manet.DialArgs(addr)
		if err != nil {
			return nil, err
		}

		l, err := net.Listen(proxyNet, proxyAddr)
		if err != nil {
			return nil, err
		}
		listeners = append(listeners, l)
	}

	nodeScheme := "http"
	if cfg.NodeHTTPS {
		nodeScheme = "https"
	}
	nodeHTTPAddr := fmt.Sprintf("%s://%s", nodeScheme, nodeAddr)
	proxyURL, err := url.Parse(nodeHTTPAddr)
	if err != nil {
		return nil, err
	}

	var handler http.Handler
	router := mux.NewRouter()
	handler = router

	if cfg.Tracing {
		handler = &ochttp.Handler{
			IsPublicEndpoint: true,
			Propagation:      &tracecontext.HTTPFormat{},
			Handler:          router,
			StartOptions:     trace.StartOptions{SpanKind: trace.SpanKindServer},
			FormatSpanName: func(req *http.Request) string {
				return "proxy:" + req.Host + ":" + req.URL.Path + ":" + req.Method
			},
		}
	}

	var writer io.Writer
	if cfg.LogFile != "" {
		f, err := os.OpenFile(cfg.getLogPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
		writer = f
	} else {
		writer = logWriter{}
	}

	s := &http.Server{
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		Handler:           handlers.LoggingHandler(writer, handler),
		MaxHeaderBytes:    cfg.MaxHeaderBytes,
	}

	// See: https://github.com/ipfs/go-ipfs/issues/5168
	// See: https://github.com/ipfs-cluster/ipfs-cluster/issues/548
	// on why this is re-enabled.
	s.SetKeepAlivesEnabled(true) // A reminder that this can be changed

	reverseProxy := httputil.NewSingleHostReverseProxy(proxyURL)
	reverseProxy.Transport = http.DefaultTransport
	ctx, cancel := context.WithCancel(context.Background())
	proxy := &Server{
		ctx:          ctx,
		config:       cfg,
		cancel:       cancel,
		nodeAddr:     nodeHTTPAddr,
		nodeScheme:   nodeScheme,
		rpcReady:     make(chan struct{}, 1),
		listeners:    listeners,
		server:       s,
		reverseProxy: reverseProxy,
	}

	// Ideally, we should only intercept POST requests, but
	// people may be calling the API with GET or worse, PUT
	// because IPFS has been allowing this traditionally.
	// The main idea here is that we do not intercept
	// OPTIONS requests (or HEAD).
	hijackSubrouter := router.
		Methods(http.MethodPost, http.MethodGet, http.MethodPut).
		PathPrefix("/api/v0").
		Subrouter()

	// Add hijacked routes
	hijackSubrouter.
		Path("/pin/add/{arg}").
		HandlerFunc(slashHandler(proxy.pinHandler)).
		Name("PinAddSlash") // supports people using the API wrong.
	hijackSubrouter.
		Path("/pin/add").
		HandlerFunc(proxy.pinHandler).
		Name("PinAdd")
	hijackSubrouter.
		Path("/pin/rm/{arg}").
		HandlerFunc(slashHandler(proxy.unpinHandler)).
		Name("PinRmSlash") // supports people using the API wrong.
	hijackSubrouter.
		Path("/pin/rm").
		HandlerFunc(proxy.unpinHandler).
		Name("PinRm")
	hijackSubrouter.
		Path("/pin/ls/{arg}").
		HandlerFunc(slashHandler(proxy.pinLsHandler)).
		Name("PinLsSlash") // supports people using the API wrong.
	hijackSubrouter.
		Path("/pin/ls").
		HandlerFunc(proxy.pinLsHandler).
		Name("PinLs")
	hijackSubrouter.
		Path("/pin/update").
		HandlerFunc(proxy.pinUpdateHandler).
		Name("PinUpdate")
	hijackSubrouter.
		Path("/add").
		HandlerFunc(proxy.addHandler).
		Name("Add")
	hijackSubrouter.
		Path("/repo/stat").
		HandlerFunc(proxy.repoStatHandler).
		Name("RepoStat")
	hijackSubrouter.
		Path("/repo/gc").
		HandlerFunc(proxy.repoGCHandler).
		Name("RepoGC")
	hijackSubrouter.
		Path("/block/put").
		HandlerFunc(proxy.blockPutHandler).
		Name("BlockPut")
	hijackSubrouter.
		Path("/dag/put").
		HandlerFunc(proxy.dagPutHandler).
		Name("DagPut")

	// Everything else goes to the IPFS daemon.
	router.PathPrefix("/").Handler(reverseProxy)

	go proxy.run()
	return proxy, nil
}

// SetClient makes the component ready to perform RPC
// requests.
func (proxy *Server) SetClient(c *rpc.Client) {
	proxy.rpcClient = c
	proxy.rpcReady <- struct{}{}
}

// Shutdown stops any listeners and stops the component from taking
// any requests.
func (proxy *Server) Shutdown(ctx context.Context) error {
	proxy.shutdownLock.Lock()
	defer proxy.shutdownLock.Unlock()

	if proxy.shutdown {
		logger.Debug("already shutdown")
		return nil
	}

	logger.Info("stopping IPFS Proxy")

	proxy.cancel()
	close(proxy.rpcReady)
	proxy.server.SetKeepAlivesEnabled(false)
	for _, l := range proxy.listeners {
		l.Close()
	}

	proxy.wg.Wait()
	proxy.shutdown = true
	return nil
}

// launches proxy when we receive the rpcReady signal.
func (proxy *Server) run() {
	<-proxy.rpcReady

	// Do not shutdown while launching threads
	// -- prevents race conditions with proxy.wg.
	proxy.shutdownLock.Lock()
	defer proxy.shutdownLock.Unlock()

	// This launches the proxy
	proxy.wg.Add(len(proxy.listeners))
	for _, l := range proxy.listeners {
		go func(l net.Listener) {
			defer proxy.wg.Done()

			maddr, err := manet.FromNetAddr(l.Addr())
			if err != nil {
				logger.Error(err)
			}

			logger.Infof(
				"IPFS Proxy: %s -> %s",
				maddr,
				proxy.config.NodeAddr,
			)
			err = proxy.server.Serve(l) // hangs here
			if err != nil && !strings.Contains(err.Error(), "closed network connection") {
				logger.Error(err)
			}
		}(l)
	}
}
