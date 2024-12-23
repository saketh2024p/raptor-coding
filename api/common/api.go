// Package common implements all the things that an IPFS Cluster API component
// must do, except the actual routes that it handles.
//
// This is meant for re-use when implementing actual REST APIs by saving most
// of the efforts and automatically getting a lot of the setup and things like
// authentication handled.
//
// The API exposes the routes in two ways: the first is through a regular
// HTTP(s) listener. The second is by tunneling HTTP through a libp2p stream
// (thus getting an encrypted channel without the need to setup TLS). Both
// ways can be used at the same time, or disabled.
//
// This is used by rest and pinsvc packages.
package common

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	jwt "github.com/golang-jwt/jwt/v4"
	types "github.com/ipfs-cluster/ipfs-cluster/api"
	raptor "github.com/ipfs-cluster/ipfs-cluster/adder/raptor" // Raptor codes integration
	state "github.com/ipfs-cluster/ipfs-cluster/state"
	logging "github.com/ipfs/go-log/v2"
	libp2p "github.com/libp2p/go-libp2p"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	gostream "github.com/libp2p/go-libp2p-gostream"
	p2phttp "github.com/libp2p/go-libp2p-http"
	host "github.com/libp2p/go-libp2p/core/host"
	peer "github.com/libp2p/go-libp2p/core/peer"
	noise "github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
	manet "github.com/multiformats/go-multiaddr/net"

	handlers "github.com/gorilla/handlers"
	mux "github.com/gorilla/mux"
	"github.com/rs/cors"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/plugin/ochttp/propagation/tracecontext"
	"go.opencensus.io/trace"
)

// StreamChannelSize is used to define buffer sizes for channels.
const StreamChannelSize = 1024

// Common errors
var (
	// ErrNoEndpointEnabled is returned when the API is created but
	// no HTTPListenAddr, nor libp2p configuration fields, nor a libp2p
	// Host are provided.
	ErrNoEndpointsEnabled = errors.New("neither the libp2p nor the HTTP endpoints are enabled")

	// ErrHTTPEndpointNotEnabled is returned when trying to perform
	// operations that rely on the HTTPEndpoint but it is disabled.
	ErrHTTPEndpointNotEnabled = errors.New("the HTTP endpoint is not enabled")
)

// SetStatusAutomatically can be passed to SendResponse(), so that it will
// figure out which http status to set by itself.
const SetStatusAutomatically = -1

// API implements an API and aims to provides
// a RESTful HTTP API for Cluster.
type API struct {
	ctx    context.Context
	cancel func()

	config *Config

	rpcClient *rpc.Client
	rpcReady  chan struct{}
	router    *mux.Router
	routes    func(*rpc.Client) []Route

	server *http.Server
	host   host.Host

	httpListeners  []net.Listener
	libp2pListener net.Listener

	shutdownLock sync.Mutex
	shutdown     bool
	wg           sync.WaitGroup
}

// Route defines a REST endpoint supported by this API.
type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc http.HandlerFunc
}

// Additional Raptor code handlers
func (api *API) EncodeRaptorHandler(w http.ResponseWriter, r *http.Request) {
	// Example handler for encoding Raptor code
	data := r.URL.Query().Get("data")
	if data == "" {
		api.SendResponse(w, http.StatusBadRequest, errors.New("data query parameter required"), nil)
		return
	}
	encoded, err := raptor.Encode([]byte(data))
	if err != nil {
		api.SendResponse(w, http.StatusInternalServerError, err, nil)
		return
	}
	api.SendResponse(w, http.StatusOK, nil, encoded)
}

func (api *API) DecodeRaptorHandler(w http.ResponseWriter, r *http.Request) {
	// Example handler for decoding Raptor code
	data := r.URL.Query().Get("data")
	if data == "" {
		api.SendResponse(w, http.StatusBadRequest, errors.New("data query parameter required"), nil)
		return
	}
	decoded, err := raptor.Decode([]byte(data))
	if err != nil {
		api.SendResponse(w, http.StatusInternalServerError, err, nil)
		return
	}
	api.SendResponse(w, http.StatusOK, nil, decoded)
}

// The rest of the API functions are unchanged but include the Raptor-specific
// endpoints in the routing logic.
