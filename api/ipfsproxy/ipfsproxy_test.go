package ipfsproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs-cluster/ipfs-cluster/test"

	cid "github.com/ipfs/go-cid"
	cmd "github.com/ipfs/go-ipfs-cmds"
	logging "github.com/ipfs/go-log/v2"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

func init() {
	_ = logging.Logger
}

func testIPFSProxyWithConfig(t *testing.T, cfg *Config) (*Server, *test.IpfsMock) {
	mock := test.NewIpfsMock(t)
	nodeMAddr, _ := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d",
		mock.Addr, mock.Port))
	proxyMAddr, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/0")

	cfg.NodeAddr = nodeMAddr
	cfg.ListenAddr = []ma.Multiaddr{proxyMAddr}
	cfg.ExtractHeadersExtra = []string{
		test.IpfsCustomHeaderName,
		test.IpfsTimeHeaderName,
	}

	// Initialize the Raptor-specific headers if needed.
	cfg.RaptorHeaders = []string{
		"X-Raptor-Coding",
		"X-Raptor-Session",
	}

	proxy, err := New(cfg)
	if err != nil {
		t.Fatal("creating an IPFSProxy should work: ", err)
	}

	proxy.server.SetKeepAlivesEnabled(false)
	proxy.SetClient(test.NewMockRPCClient(t))
	return proxy, mock
}

func testIPFSProxy(t *testing.T) (*Server, *test.IpfsMock) {
	cfg := &Config{}
	cfg.Default()
	return testIPFSProxyWithConfig(t, cfg)
}

func TestRaptorHeaders(t *testing.T) {
	ctx := context.Background()
	proxy, mock := testIPFSProxy(t)
	defer mock.Close()
	defer proxy.Shutdown(ctx)

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/version", proxyURL(proxy)), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Raptor-Coding", "Enabled")
	req.Header.Set("X-Raptor-Session", "Session12345")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, res.StatusCode)
	}

	hasRaptorHeader := false
	for key := range res.Header {
		if strings.HasPrefix(key, "X-Raptor") {
			hasRaptorHeader = true
			break
		}
	}

	if !hasRaptorHeader {
		t.Error("Raptor headers are not processed correctly")
	}
}

// Other test cases remain unchanged unless explicitly requiring Raptor-related modifications

func proxyURL(c *Server) string {
	addr := c.listeners[0].Addr()
	return fmt.Sprintf("http://%s/api/v0", addr)
}