package config

import (
	"flag"
	yaml "gopkg.in/yaml.v3"
	"os"
	"path/filepath"
)

const (
	EngineTypeMemory = "in_memory"
	NetworkAddress   = ":3223"
	MaxConnections   = 1
	LoggingLevel     = "debug"
	LoggingOutput    = "console"
)

type Config struct {
	Engine  *EngineConfig  `yaml:"engine"`
	Network *NetworkConfig `yaml:"network"`
	Logging *LoggingConfig `yaml:"logging"`
}

type EngineConfig struct {
	Type string `yaml:"type"`
}

type NetworkConfig struct {
	Address        string `yaml:"address"`
	MaxConnections int    `yaml:"max_connections"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Output string `yaml:"output"`
}

func GetConfig() (*Config, error) {
	var configPath string
	flag.StringVar(&configPath, "c", "config.yaml", "Used for set path to config file.")
	flag.Parse()

	data, err := os.ReadFile(filepath.Clean(configPath))
	if err != nil {
		return nil, err
	}
	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}
	setDefaults(&cfg)

	return &cfg, err
}

func setDefaults(cfg *Config) {
	if cfg.Engine.Type == "" {
		cfg.Engine.Type = EngineTypeMemory
	}
	if cfg.Network.Address == "" {
		cfg.Network.Address = NetworkAddress
	}
	if cfg.Network.MaxConnections == 0 {
		cfg.Network.MaxConnections = MaxConnections
	}
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = LoggingLevel
	}
	if cfg.Logging.Output == "" {
		cfg.Logging.Output = LoggingOutput
	}
}
