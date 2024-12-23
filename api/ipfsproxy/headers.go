package ipfsproxy

import (
	"fmt"
	"net/http"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/version"
)

// CORS Headers for Raptor coding
var corsHeaders = []string{
	"Access-Control-Allow-Origin",
	"Vary",
	"Access-Control-Allow-Credentials",
	"Access-Control-Expose-Headers",
}

// Default headers for Raptor coding
var extractHeadersDefault = []string{
	"Access-Control-Expose-Headers",
	"X-Raptor-Code-Version", // Raptor-specific header
	"X-Raptor-Coding-Scheme", // Raptor coding scheme identifier
}

const ipfsHeadersTimestampKey = "proxyHeadersTS"

// ipfsHeaders returns all the headers we want to extract-once from IPFS.
func (proxy *Server) ipfsHeaders() []string {
	return append(extractHeadersDefault, proxy.config.ExtractHeadersExtra...)
}

// rememberIPFSHeaders extracts headers and stores them for re-use.
func (proxy *Server) rememberIPFSHeaders(hdrs http.Header) {
	for _, h := range proxy.ipfsHeaders() {
		proxy.ipfsHeadersStore.Store(h, hdrs[h])
	}
	proxy.ipfsHeadersStore.Store(ipfsHeadersTimestampKey, time.Now())
}

// headersWithinTTL checks whether stored headers are within their TTL.
func (proxy *Server) headersWithinTTL() bool {
	ttl := proxy.config.ExtractHeadersTTL
	if ttl == 0 {
		return true
	}

	tsRaw, ok := proxy.ipfsHeadersStore.Load(ipfsHeadersTimestampKey)
	if !ok {
		return false
	}

	ts, ok := tsRaw.(time.Time)
	if !ok {
		return false
	}

	return time.Since(ts) < ttl
}

// setIPFSHeaders adds known IPFS headers to the destination.
func (proxy *Server) setIPFSHeaders(dest http.Header) bool {
	r := true

	if !proxy.headersWithinTTL() {
		r = false
	}

	for _, h := range proxy.ipfsHeaders() {
		v, ok := proxy.ipfsHeadersStore.Load(h)
		if !ok {
			r = false
			continue
		}
		dest[h] = v.([]string)
	}
	return r
}

// copyHeadersFromIPFSWithRequest copies specific headers from an IPFS request.
func (proxy *Server) copyHeadersFromIPFSWithRequest(
	hdrs []string,
	dest http.Header, req *http.Request,
) error {
	res, err := proxy.reverseProxy.Transport.RoundTrip(req)
	if err != nil {
		logger.Error("error making request for header extraction to ipfs: ", err)
		return err
	}

	for _, h := range hdrs {
		dest[h] = res.Header[h]
	}
	return nil
}

// setHeaders manages headers for all hijacked endpoints.
func (proxy *Server) setHeaders(dest http.Header, srcRequest *http.Request) {
	proxy.setCORSHeaders(dest, srcRequest)
	proxy.setAdditionalIpfsHeaders(dest, srcRequest)
	proxy.setClusterProxyHeaders(dest, srcRequest)
}

// setCORSHeaders manages CORS headers for the proxy.
func (proxy *Server) setCORSHeaders(dest http.Header, srcRequest *http.Request) {
	srcURL := fmt.Sprintf("%s%s", proxy.nodeAddr, srcRequest.URL.Path)
	req, err := http.NewRequest(http.MethodOptions, srcURL, nil)
	if err != nil {
		logger.Error(err)
		return
	}

	req.Header["Origin"] = srcRequest.Header["Origin"]
	req.Header.Set("Access-Control-Request-Method", srcRequest.Method)
	proxy.copyHeadersFromIPFSWithRequest(corsHeaders, dest, req)
}

// setAdditionalIpfsHeaders manages additional headers from IPFS.
func (proxy *Server) setAdditionalIpfsHeaders(dest http.Header, srcRequest *http.Request) {
	if ok := proxy.setIPFSHeaders(dest); ok {
		return
	}

	srcURL := fmt.Sprintf("%s%s", proxy.nodeAddr, proxy.config.ExtractHeadersPath)
	req, err := http.NewRequest(http.MethodPost, srcURL, nil)
	if err != nil {
		logger.Error("error extracting additional headers from ipfs", err)
		return
	}

	proxy.copyHeadersFromIPFSWithRequest(
		proxy.ipfsHeaders(),
		dest,
		req,
	)
	proxy.rememberIPFSHeaders(dest)
}

// setClusterProxyHeaders adds Raptor-specific headers.
func (proxy *Server) setClusterProxyHeaders(dest http.Header, srcRequest *http.Request) {
	dest.Set("Content-Type", "application/json")
	dest.Set("Server", fmt.Sprintf("ipfs-cluster/ipfsproxy/%s", version.Version))
	dest.Set("X-Raptor-Code-Version", "1.0")       // Example header
	dest.Set("X-Raptor-Coding-Scheme", "FEC-Raptor") // Example header
}
