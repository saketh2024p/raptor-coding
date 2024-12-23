package balanced

import (
	"os"
	"testing"
)

var cfgJSON = []byte(`
{
      "allocate_by": ["tag", "disk"],
      "raptor_params": {
          "block_size": 2048,
          "coding_rate": 0.7,
          "redundancy": 3
      }
}
`)

func TestLoadJSON(t *testing.T) {
	cfg := &Config{}
	err := cfg.LoadJSON(cfgJSON)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.RaptorParams.BlockSize != 2048 {
		t.Error("failed to load RaptorConfig block_size")
	}
	if cfg.RaptorParams.CodingRate != 0.7 {
		t.Error("failed to load RaptorConfig coding_rate")
	}
	if cfg.RaptorParams.Redundancy != 3 {
		t.Error("failed to load RaptorConfig redundancy")
	}
}

func TestToJSON(t *testing.T) {
	cfg := &Config{}
	err := cfg.LoadJSON(cfgJSON)
	if err != nil {
		t.Fatal(err)
	}
	newJSON, err := cfg.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	cfg = &Config{}
	err = cfg.LoadJSON(newJSON)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.AllocateBy) != 2 {
		t.Error("configuration was lost in serialization/deserialization")
	}
	if cfg.RaptorParams.BlockSize != 2048 {
		t.Error("RaptorConfig block_size was lost in serialization/deserialization")
	}
	if cfg.RaptorParams.CodingRate != 0.7 {
		t.Error("RaptorConfig coding_rate was lost in serialization/deserialization")
	}
	if cfg.RaptorParams.Redundancy != 3 {
		t.Error("RaptorConfig redundancy was lost in serialization/deserialization")
	}
}

func TestDefault(t *testing.T) {
	cfg := &Config{}
	cfg.Default()
	if cfg.Validate() != nil {
		t.Fatal("error validating default configuration")
	}

	if cfg.RaptorParams.BlockSize != DefaultRaptorParams.BlockSize {
		t.Error("default RaptorConfig block_size is incorrect")
	}
	if cfg.RaptorParams.CodingRate != DefaultRaptorParams.CodingRate {
		t.Error("default RaptorConfig coding_rate is incorrect")
	}
	if cfg.RaptorParams.Redundancy != DefaultRaptorParams.Redundancy {
		t.Error("default RaptorConfig redundancy is incorrect")
	}

	cfg.AllocateBy = nil
	if cfg.Validate() == nil {
		t.Fatal("expected error validating invalid AllocateBy")
	}

	cfg.RaptorParams.BlockSize = -1
	if cfg.Validate() == nil {
		t.Fatal("expected error validating invalid RaptorConfig block_size")
	}

	cfg.RaptorParams.CodingRate = 1.5
	if cfg.Validate() == nil {
		t.Fatal("expected error validating invalid RaptorConfig coding_rate")
	}

	cfg.RaptorParams.Redundancy = -1
	if cfg.Validate() == nil {
		t.Fatal("expected error validating invalid RaptorConfig redundancy")
	}
}

func TestApplyEnvVars(t *testing.T) {
	os.Setenv("CLUSTER_BALANCED_ALLOCATEBY", "a,b,c")
	os.Setenv("CLUSTER_BALANCED_RAPTORPARAMS_BLOCKSIZE", "4096")
	os.Setenv("CLUSTER_BALANCED_RAPTORPARAMS_CODINGRATE", "0.8")
	os.Setenv("CLUSTER_BALANCED_RAPTORPARAMS_REDUNDANCY", "4")

	cfg := &Config{}
	err := cfg.ApplyEnvVars()
	if err != nil {
		t.Fatal(err)
	}

	if len(cfg.AllocateBy) != 3 {
		t.Fatal("failed to override allocate_by with env var")
	}
	if cfg.RaptorParams.BlockSize != 4096 {
		t.Fatal("failed to override RaptorConfig block_size with env var")
	}
	if cfg.RaptorParams.CodingRate != 0.8 {
		t.Fatal("failed to override RaptorConfig coding_rate with env var")
	}
	if cfg.RaptorParams.Redundancy != 4 {
		t.Fatal("failed to override RaptorConfig redundancy with env var")
	}
}
