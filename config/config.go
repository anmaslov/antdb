package config

import (
	"flag"
	yaml "gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"time"
)

const (
	EngineTypeMemory = "in_memory"
	NetworkAddress   = ":3223"
	MasterAddress    = ":3232"
	MaxConnections   = 1
	MessageSize      = "1KB"
	LoggingLevel     = "debug"
	LoggingOutput    = "console"
)

type Config struct {
	Engine            *EngineConfig      `yaml:"engine"`
	Network           *NetworkConfig     `yaml:"network"`
	Logging           *LoggingConfig     `yaml:"logging"`
	WAL               *WALConfig         `yaml:"wal"`
	ReplicationConfig *ReplicationConfig `yaml:"replication"`
}

type EngineConfig struct {
	Type string `yaml:"type"`
}

type NetworkConfig struct {
	Address        string `yaml:"address"`
	MaxConnections int    `yaml:"max_connections"`
	MessageSize    string `yaml:"message_size"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Output string `yaml:"output"`
}

type WALConfig struct {
	FlushingBatchSize    int           `yaml:"flushing_batch_size"`
	FlushingBatchTimeout time.Duration `yaml:"flushing_batch_timeout"`
	MaxSegmentSize       string        `yaml:"max_segment_size"`
	DataDirectory        string        `yaml:"data_directory"`
}

type ReplicationConfig struct {
	ReplicaType   string        `yaml:"replica_type"`
	MasterAddress string        `yaml:"master_address"`
	SyncInterval  time.Duration `yaml:"sync_interval"`
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
	if cfg.Network.MessageSize == "" {
		cfg.Network.MessageSize = MessageSize
	}
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = LoggingLevel
	}
	if cfg.Logging.Output == "" {
		cfg.Logging.Output = LoggingOutput
	}
	if cfg.WAL.FlushingBatchSize == 0 {
		cfg.WAL.FlushingBatchSize = 5
	}
	if cfg.WAL.FlushingBatchTimeout == 0 {
		cfg.WAL.FlushingBatchTimeout = 10 * time.Millisecond
	}
	if cfg.WAL.MaxSegmentSize == "" {
		cfg.WAL.MaxSegmentSize = "5MB"
	}
	if cfg.WAL.DataDirectory == "" {
		cfg.WAL.DataDirectory = "-"
	}
	if cfg.ReplicationConfig.ReplicaType == "" {
		cfg.ReplicationConfig.ReplicaType = "slave"
	}
	if cfg.ReplicationConfig.SyncInterval == 0 {
		cfg.ReplicationConfig.SyncInterval = time.Second
	}
	if cfg.ReplicationConfig.MasterAddress == "" {
		cfg.ReplicationConfig.MasterAddress = MasterAddress
	}
}
