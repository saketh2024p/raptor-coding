package common

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs-cluster/ipfs-cluster/api/common/test"
	rpctest "github.com/ipfs-cluster/ipfs-cluster/test"

	libp2p "github.com/libp2p/go-libp2p"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	SSLCertFile         = "test/server.crt"
	SSLKeyFile          = "test/server.key"
	validUserName       = "validUserName"
	validUserPassword   = "validUserPassword"
	adminUserName       = "adminUserName"
	adminUserPassword   = "adminUserPassword"
	invalidUserName     = "invalidUserName"
	invalidUserPassword = "invalidUserPassword"
)

var (
	validToken, _   = generateSignedTokenString(validUserName, validUserPassword)
	invalidToken, _ = generateSignedTokenString(invalidUserName, invalidUserPassword)
)

func routes(c *rpc.Client) []Route {
	return []Route{
		{
			"Test",
			"GET",
			"/test",
			func(w http.ResponseWriter, r *http.Request) {
				w.Header().Add("Content-Type", "application/json")
				w.Write([]byte(`{ "thisis": "atest" }`))
			},
		},
	}
}

func testAPIwithConfig(t *testing.T, cfg *Config, name string) *API {
	ctx := context.Background()
	apiMAddr, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	h, err := libp2p.New(libp2p.ListenAddrs(apiMAddr))
	if err != nil {
		t.Fatal(err)
	}

	cfg.HTTPListenAddr = []ma.Multiaddr{apiMAddr}

	rest, err := NewAPIWithHost(ctx, cfg, h, routes)
	if err != nil {
		t.Fatalf("should be able to create a new %s API: %s", name, err)
	}

	// No keep alive for tests
	rest.server.SetKeepAlivesEnabled(false)
	rest.SetClient(rpctest.NewMockRPCClient(t))

	return rest
}

func testAPI(t *testing.T) *API {
	cfg := newDefaultTestConfig(t)
	cfg.CORSAllowedOrigins = []string{test.ClientOrigin}
	cfg.CORSAllowedMethods = []string{"GET", "POST", "DELETE"}
	cfg.CORSMaxAge = 10 * time.Minute

	return testAPIwithConfig(t, cfg, "basic")
}

func testHTTPSAPI(t *testing.T) *API {
	cfg := newDefaultTestConfig(t)
	cfg.PathSSLCertFile = SSLCertFile
	cfg.PathSSLKeyFile = SSLKeyFile
	var err error
	cfg.TLS, err = newTLSConfig(cfg.PathSSLCertFile, cfg.PathSSLKeyFile)
	if err != nil {
		t.Fatal(err)
	}

	return testAPIwithConfig(t, cfg, "https")
}

func TestAPIShutdown(t *testing.T) {
	ctx := context.Background()
	rest := testAPI(t)
	err := rest.Shutdown(ctx)
	if err != nil {
		t.Error("should shutdown cleanly: ", err)
	}
	// test shutting down twice
	rest.Shutdown(ctx)
}

func TestHTTPSTestEndpoint(t *testing.T) {
	ctx := context.Background()
	rest := testAPI(t)
	httpsrest := testHTTPSAPI(t)
	defer rest.Shutdown(ctx)
	defer httpsrest.Shutdown(ctx)

	tf := func(t *testing.T, url test.URLFunc) {
		r := make(map[string]string)
		test.MakeGet(t, rest, url(rest)+"/test", &r)
		if r["thisis"] != "atest" {
			t.Error("expected correct body")
		}
	}

	httpstf := func(t *testing.T, url test.URLFunc) {
		r := make(map[string]string)
		test.MakeGet(t, httpsrest, url(httpsrest)+"/test", &r)
		if r["thisis"] != "atest" {
			t.Error("expected correct body")
		}
	}

	test.BothEndpoints(t, tf)
	test.HTTPSEndPoint(t, httpstf)
}

func TestAPILogging(t *testing.T) {
	ctx := context.Background()
	cfg := newDefaultTestConfig(t)

	logFile, err := filepath.Abs("http.log")
	if err != nil {
		t.Fatal(err)
	}
	cfg.HTTPLogFile = logFile

	rest := testAPIwithConfig(t, cfg, "log_enabled")
	defer os.Remove(cfg.HTTPLogFile)

	info, err := os.Stat(cfg.HTTPLogFile)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 0 {
		t.Errorf("expected empty log file")
	}

	id := api.ID{}
	test.MakeGet(t, rest, test.HTTPURL(rest)+"/test", &id)

	info, err = os.Stat(cfg.HTTPLogFile)
	if err != nil {
		t.Fatal(err)
	}
	size1 := info.Size()
	if size1 == 0 {
		t.Error("did not expect an empty log file")
	}

	// Restart API and make sure that logs are being appended
	rest.Shutdown(ctx)

	rest = testAPIwithConfig(t, cfg, "log_enabled")
	defer rest.Shutdown(ctx)

	test.MakeGet(t, rest, test.HTTPURL(rest)+"/id", &id)

	info, err = os.Stat(cfg.HTTPLogFile)
	if err != nil {
		t.Fatal(err)
	}
	size2 := info.Size()
	if size2 == 0 {
		t.Error("did not expect an empty log file")
	}

	if !(size2 > size1) {
		t.Error("logs were not appended")
	}
}

func TestNotFoundHandler(t *testing.T) {
	ctx := context.Background()
	rest := testAPI(t)
	defer rest.Shutdown(ctx)

	tf := func(t *testing.T, url test.URLFunc) {
		bytes := make([]byte, 10)
		for i := 0; i < 10; i++ {
			bytes[i] = byte(65 + rand.Intn(25)) //A=65 and Z = 65+25
		}

		var errResp api.Error
		test.MakePost(t, rest, url(rest)+"/"+string(bytes), []byte{}, &errResp)
		if errResp.Code != 404 {
			t.Errorf("expected error not found: %+v", errResp)
		}

		var errResp1 api.Error
		test.MakeGet(t, rest, url(rest)+"/"+string(bytes), &errResp1)
		if errResp1.Code != 404 {
			t.Errorf("expected error not found: %+v", errResp)
		}
	}
	test.BothEndpoints(t, tf)
}

func TestCORS(t *testing.T) {
	ctx := context.Background()
	rest := testAPI(t)
	defer rest.Shutdown(ctx)

	type testcase struct {
		method string
		path   string
	}

tf := func(t *testing.T, url test.URLFunc) {
		reqHeaders := make(http.Header)
		reqHeaders.Set("Origin", "myorigin")
		reqHeaders.Set("Access-Control-Request-Headers", "Content-Type")

		for _, tc := range []testcase{
			{"GET", "/test"},
		} {
			reqHeaders.Set("Access-Control-Request-Method", tc.method)
			headers := test.MakeOptions(t, rest, url(rest)+tc.path, reqHeaders)
			aorigin := headers.Get("Access-Control-Allow-Origin")
			amethods := headers.Get("Access-Control-Allow-Methods")
			aheaders := headers.Get("Access-Control-Allow-Headers")
			acreds := headers.Get("Access-Control-Allow-Credentials")
			maxage := headers.Get("Access-Control-Max-Age")

			if aorigin != "myorigin" {
				t.Error("Bad ACA-Origin:", aorigin)
			}

			if amethods != tc.method {
				t.Error("Bad ACA-Methods:", amethods)
			}

			if aheaders != "Content-Type" {
				t.Error("Bad ACA-Headers:", aheaders)
			}

			if acreds != "true" {
				t.Error("Bad ACA-Credentials:", acreds)
			}

			if maxage != "600" {
				t.Error("Bad AC-Max-Age:", maxage)
			}
		}
	}
	test.BothEndpoints(t, tf)
}
