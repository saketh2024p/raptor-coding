package pinqueue

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/config"
	"github.com/kelseyhightower/envconfig"
)

const configKey = "pinqueue"
const envConfigKey = "cluster_pinqueue"

// These are the default values for a Config.
const (
	DefaultMetricTTL        = 30 * time.Second
	DefaultWeightBucketSize = 100000 // 100k pins
)

// Config allows to initialize an Informer.
type Config struct {
	config.Saver

	MetricTTL        time.Duration
	WeightBucketSize int
}

type jsonConfig struct {
	MetricTTL        string `json:"metric_ttl"`
	WeightBucketSize int    `json:"weight_bucket_size"`
}

// ConfigKey returns a human-friendly identifier for this
// Config's type.
func (cfg *Config) ConfigKey() string {
	return configKey
}

// Default initializes this Config with sensible values.
func (cfg *Config) Default() error {
	cfg.MetricTTL = DefaultMetricTTL
	cfg.WeightBucketSize = DefaultWeightBucketSize
	return nil
}

// ApplyEnvVars fills in any Config fields found
// as environment variables.
func (cfg *Config) ApplyEnvVars() error {
	jcfg := cfg.toJSONConfig()

	err := envconfig.Process(envConfigKey, jcfg)
	if err != nil {
		return err
	}

	return cfg.applyJSONConfig(jcfg)
}

// Validate checks that the fields of this configuration have
// sensible values.
func (cfg *Config) Validate() error {
	if cfg.MetricTTL <= 0 {
		return errors.New("pinqueue.metric_ttl is invalid")
	}
	if cfg.WeightBucketSize < 0 {
		return errors.New("pinqueue.WeightBucketSize is invalid")
	}

	return nil
}

// LoadJSON parses a raw JSON byte-slice as generated by ToJSON().
func (cfg *Config) LoadJSON(raw []byte) error {
	jcfg := &jsonConfig{}
	err := json.Unmarshal(raw, jcfg)
	if err != nil {
		return err
	}

	cfg.Default()

	return cfg.applyJSONConfig(jcfg)
}

func (cfg *Config) applyJSONConfig(jcfg *jsonConfig) error {
	t, _ := time.ParseDuration(jcfg.MetricTTL)
	cfg.MetricTTL = t
	cfg.WeightBucketSize = jcfg.WeightBucketSize

	return cfg.Validate()
}

// ToJSON generates a human-friendly JSON representation of this Config.
func (cfg *Config) ToJSON() ([]byte, error) {
	jcfg := cfg.toJSONConfig()

	return config.DefaultJSONMarshal(jcfg)
}

func (cfg *Config) toJSONConfig() *jsonConfig {
	return &jsonConfig{
		MetricTTL:        cfg.MetricTTL.String(),
		WeightBucketSize: cfg.WeightBucketSize,
	}
}

// ToDisplayJSON returns JSON config as a string.
func (cfg *Config) ToDisplayJSON() ([]byte, error) {
	return config.DisplayJSON(cfg.toJSONConfig())
}