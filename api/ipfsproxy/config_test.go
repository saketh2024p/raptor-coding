package ipfsproxy

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

var cfgJSON = []byte(`
{
	"listen_multiaddress": "/ip4/127.0.0.1/tcp/9095",
	"node_multiaddress": "/ip4/127.0.0.1/tcp/5001",
	"log_file": "",
	"read_timeout": "10m0s",
	"read_header_timeout": "5s",
	"write_timeout": "10m0s",
	"idle_timeout": "1m0s",
	"max_header_bytes": 16384,
	"extract_headers_extra": [],
	"extract_headers_path": "/api/v0/version",
	"extract_headers_ttl": "5m",
	"raptor_code_version": "1.0",
	"raptor_coding_scheme": "FEC-Raptor"
}
`)

func TestLoadEmptyJSON(t *testing.T) {
	cfg := &Config{}
	err := cfg.LoadJSON([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoadJSON(t *testing.T) {
	cfg := &Config{}
	err := cfg.LoadJSON(cfgJSON)
	if err != nil {
		t.Fatal(err)
	}

	j := &jsonConfig{}
	json.Unmarshal(cfgJSON, j)
	j.ListenMultiaddress = []string{"abc"}
	tst, _ := json.Marshal(j)
	err = cfg.LoadJSON(tst)
	if err == nil {
		t.Error("expected error decoding listen_multiaddress")
	}

	j = &jsonConfig{}
	json.Unmarshal(cfgJSON, j)
	j.NodeMultiaddress = "abc"
	tst, _ = json.Marshal(j)
	err = cfg.LoadJSON(tst)
	if err == nil {
		t.Error("expected error in node_multiaddress")
	}

	j = &jsonConfig{}
	json.Unmarshal(cfgJSON, j)
	j.ReadTimeout = "-aber"
	tst, _ = json.Marshal(j)
	err = cfg.LoadJSON(tst)
	if err == nil {
		t.Error("expected error in read_timeout")
	}

	j = &jsonConfig{}
	json.Unmarshal(cfgJSON, j)
	j.ExtractHeadersTTL = "-10"
	tst, _ = json.Marshal(j)
	err = cfg.LoadJSON(tst)
	if err == nil {
		t.Error("expected error in extract_headers_ttl")
	}

	j = &jsonConfig{}
	json.Unmarshal(cfgJSON, j)
	j.MaxHeaderBytes = minMaxHeaderBytes - 1
	tst, _ = json.Marshal(j)
	err = cfg.LoadJSON(tst)
	if err == nil {
		t.Error("expected error in max_header_bytes")
	}

	// Test invalid Raptor coding fields
	j = &jsonConfig{}
	json.Unmarshal(cfgJSON, j)
	j.RaptorCodeVersion = ""
	tst, _ = json.Marshal(j)
	err = cfg.LoadJSON(tst)
	if err == nil {
		t.Error("expected error in raptor_code_version")
	}

	j = &jsonConfig{}
	json.Unmarshal(cfgJSON, j)
	j.RaptorCodingScheme = ""
	tst, _ = json.Marshal(j)
	err = cfg.LoadJSON(tst)
	if err == nil {
		t.Error("expected error in raptor_coding_scheme")
	}
}

func TestToJSON(t *testing.T) {
	cfg := &Config{}
	cfg.LoadJSON(cfgJSON)
	newjson, err := cfg.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	cfg = &Config{}
	err = cfg.LoadJSON(newjson)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDefault(t *testing.T) {
	cfg := &Config{}
	cfg.Default()
	if cfg.Validate() != nil {
		t.Fatal("error validating")
	}

	cfg.NodeAddr = nil
	if cfg.Validate() == nil {
		t.Fatal("expected error validating")
	}

	cfg.Default()
	cfg.ListenAddr = nil
	if cfg.Validate() == nil {
		t.Fatal("expected error validating")
	}

	cfg.Default()
	cfg.ReadTimeout = -1
	if cfg.Validate() == nil {
		t.Fatal("expected error validating")
	}

	cfg.Default()
	cfg.ReadHeaderTimeout = -2
	if cfg.Validate() == nil {
		t.Fatal("expected error validating")
	}

	cfg.Default()
	cfg.IdleTimeout = -1
	if cfg.Validate() == nil {
		t.Fatal("expected error validating")
	}

	cfg.Default()
	cfg.WriteTimeout = -3
	if cfg.Validate() == nil {
		t.Fatal("expected error validating")
	}

	cfg.Default()
	cfg.ExtractHeadersPath = ""
	if cfg.Validate() == nil {
		t.Fatal("expected error validating")
	}

	cfg.Default()
	cfg.RaptorCodeVersion = ""
	if cfg.Validate() == nil {
		t.Fatal("expected error validating raptor_code_version")
	}

	cfg.Default()
	cfg.RaptorCodingScheme = ""
	if cfg.Validate() == nil {
		t.Fatal("expected error validating raptor_coding_scheme")
	}
}

func TestApplyEnvVars(t *testing.T) {
	os.Setenv("CLUSTER_IPFSPROXY_IDLETIMEOUT", "22s")
	os.Setenv("CLUSTER_IPFSPROXY_RAPTOR_CODE_VERSION", "2.0")
	os.Setenv("CLUSTER_IPFSPROXY_RAPTOR_CODING_SCHEME", "FEC-RaptorX")
	cfg := &Config{}
	cfg.Default()
	cfg.ApplyEnvVars()

	if cfg.IdleTimeout != 22*time.Second {
		t.Error("failed to override idle_timeout with env var")
	}
	if cfg.RaptorCodeVersion != "2.0" {
		t.Error("failed to override raptor_code_version with env var")
	}
	if cfg.RaptorCodingScheme != "FEC-RaptorX" {
		t.Error("failed to override raptor_coding_scheme with env var")
	}
}
