package ipfsproxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/kelseyhightower/envconfig"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/ipfs-cluster/ipfs-cluster/config"
)

const (
	configKey         = "ipfsproxy"
	envConfigKey      = "cluster_ipfsproxy"
	minMaxHeaderBytes = 4096
)

// Default values for Config.
const (
	DefaultNodeAddr           = "/ip4/127.0.0.1/tcp/5001"
	DefaultNodeHTTPS          = false
	DefaultReadTimeout        = 0
	DefaultReadHeaderTimeout  = 5 * time.Second
	DefaultWriteTimeout       = 0
	DefaultIdleTimeout        = 60 * time.Second
	DefaultExtractHeadersPath = "/api/v0/version"
	DefaultExtractHeadersTTL  = 5 * time.Minute
	DefaultMaxHeaderBytes     = minMaxHeaderBytes

	// Default values for Raptor Coding
	DefaultRaptorCodeVersion = "1.0"
	DefaultRaptorCodingScheme = "FEC-Raptor"
)

// Config allows customization of IPFS Proxy behavior.
type Config struct {
	config.Saver

	// Listen parameters for the IPFS Proxy.
	ListenAddr []ma.Multiaddr

	// Host/Port for the IPFS daemon.
	NodeAddr ma.Multiaddr

	// Use HTTPS for IPFS API
	NodeHTTPS bool

	// LogFile for Proxy API logs.
	LogFile string

	// Request timeouts
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	MaxHeaderBytes    int

	// Header extraction
	ExtractHeadersExtra []string
	ExtractHeadersPath  string
	ExtractHeadersTTL   time.Duration

	// Raptor Coding fields
	RaptorCodeVersion string
	RaptorCodingScheme string

	// Tracing flag used to skip tracing specific paths when not enabled.
	Tracing bool
}

type jsonConfig struct {
	ListenMultiaddress config.Strings `json:"listen_multiaddress"`
	NodeMultiaddress   string         `json:"node_multiaddress"`
	NodeHTTPS          bool           `json:"node_https,omitempty"`

	LogFile string `json:"log_file"`

	ReadTimeout       string `json:"read_timeout"`
	ReadHeaderTimeout string `json:"read_header_timeout"`
	WriteTimeout      string `json:"write_timeout"`
	IdleTimeout       string `json:"idle_timeout"`
	MaxHeaderBytes    int    `json:"max_header_bytes"`

	ExtractHeadersExtra []string `json:"extract_headers_extra,omitempty"`
	ExtractHeadersPath  string   `json:"extract_headers_path,omitempty"`
	ExtractHeadersTTL   string   `json:"extract_headers_ttl,omitempty"`

	// Raptor Coding fields
	RaptorCodeVersion string `json:"raptor_code_version"`
	RaptorCodingScheme string `json:"raptor_coding_scheme"`
}

// Default sets default values for the Config.
func (cfg *Config) Default() error {
	proxy := make([]ma.Multiaddr, 0, len(DefaultListenAddrs))
	for _, def := range DefaultListenAddrs {
		a, err := ma.NewMultiaddr(def)
		if err != nil {
			return err
		}
		proxy = append(proxy, a)
	}
	node, err := ma.NewMultiaddr(DefaultNodeAddr)
	if err != nil {
		return err
	}
	cfg.ListenAddr = proxy
	cfg.NodeAddr = node
	cfg.LogFile = ""
	cfg.ReadTimeout = DefaultReadTimeout
	cfg.ReadHeaderTimeout = DefaultReadHeaderTimeout
	cfg.WriteTimeout = DefaultWriteTimeout
	cfg.IdleTimeout = DefaultIdleTimeout
	cfg.MaxHeaderBytes = DefaultMaxHeaderBytes
	cfg.ExtractHeadersExtra = nil
	cfg.ExtractHeadersPath = DefaultExtractHeadersPath
	cfg.ExtractHeadersTTL = DefaultExtractHeadersTTL

	// Defaults for Raptor Coding
	cfg.RaptorCodeVersion = DefaultRaptorCodeVersion
	cfg.RaptorCodingScheme = DefaultRaptorCodingScheme

	return nil
}

// Validate checks the validity of configuration values.
func (cfg *Config) Validate() error {
	if len(cfg.ListenAddr) == 0 {
		return errors.New("ipfsproxy.listen_multiaddress not set")
	}
	if cfg.NodeAddr == nil {
		return errors.New("ipfsproxy.node_multiaddress not set")
	}
	if cfg.ReadTimeout < 0 {
		return errors.New("ipfsproxy.read_timeout is invalid")
	}
	if cfg.ReadHeaderTimeout < 0 {
		return errors.New("ipfsproxy.read_header_timeout is invalid")
	}
	if cfg.WriteTimeout < 0 {
		return errors.New("ipfsproxy.write_timeout is invalid")
	}
	if cfg.IdleTimeout < 0 {
		return errors.New("ipfsproxy.idle_timeout invalid")
	}
	if cfg.ExtractHeadersPath == "" {
		return errors.New("ipfsproxy.extract_headers_path should not be empty")
	}
	if cfg.ExtractHeadersTTL < 0 {
		return errors.New("ipfsproxy.extract_headers_ttl is invalid")
	}
	if cfg.MaxHeaderBytes < minMaxHeaderBytes {
		return fmt.Errorf("ipfsproxy.max_header_bytes must be >= %d", minMaxHeaderBytes)
	}

	// Validation for Raptor Coding
	if cfg.RaptorCodeVersion == "" {
		return errors.New("raptor_code_version must be set")
	}
	if cfg.RaptorCodingScheme == "" {
		return errors.New("raptor_coding_scheme must be set")
	}

	return nil
}

// applyJSONConfig updates Config fields based on a JSON Config.
func (cfg *Config) applyJSONConfig(jcfg *jsonConfig) error {
	if addresses := jcfg.ListenMultiaddress; len(addresses) > 0 {
		cfg.ListenAddr = make([]ma.Multiaddr, 0, len(addresses))
		for _, a := range addresses {
			addr, err := ma.NewMultiaddr(a)
			if err != nil {
				return fmt.Errorf("error parsing proxy listen_multiaddress: %s", err)
			}
			cfg.ListenAddr = append(cfg.ListenAddr, addr)
		}
	}
	if jcfg.NodeMultiaddress != "" {
		addr, err := ma.NewMultiaddr(jcfg.NodeMultiaddress)
		if err != nil {
			return fmt.Errorf("error parsing node_multiaddress: %s", err)
		}
		cfg.NodeAddr = addr
	}
	config.SetIfNotDefault(jcfg.NodeHTTPS, &cfg.NodeHTTPS)

	config.SetIfNotDefault(jcfg.LogFile, &cfg.LogFile)
	config.SetIfNotDefault(jcfg.RaptorCodeVersion, &cfg.RaptorCodeVersion)
	config.SetIfNotDefault(jcfg.RaptorCodingScheme, &cfg.RaptorCodingScheme)

	err := config.ParseDurations(
		"ipfsproxy",
		&config.DurationOpt{Duration: jcfg.ReadTimeout, Dst: &cfg.ReadTimeout, Name: "read_timeout"},
		&config.DurationOpt{Duration: jcfg.ReadHeaderTimeout, Dst: &cfg.ReadHeaderTimeout, Name: "read_header_timeout"},
		&config.DurationOpt{Duration: jcfg.WriteTimeout, Dst: &cfg.WriteTimeout, Name: "write_timeout"},
		&config.DurationOpt{Duration: jcfg.IdleTimeout, Dst: &cfg.IdleTimeout, Name: "idle_timeout"},
		&config.DurationOpt{Duration: jcfg.ExtractHeadersTTL, Dst: &cfg.ExtractHeadersTTL, Name: "extract_headers_ttl"},
	)
	if err != nil {
		return err
	}

	if extra := jcfg.ExtractHeadersExtra; len(extra) > 0 {
		cfg.ExtractHeadersExtra = extra
	}
	config.SetIfNotDefault(jcfg.ExtractHeadersPath, &cfg.ExtractHeadersPath)

	return cfg.Validate()
}
