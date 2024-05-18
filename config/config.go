package config

import (
	"flag"
	yaml "gopkg.in/yaml.v3"
	"os"
	"path/filepath"
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

	return &cfg, err
}
