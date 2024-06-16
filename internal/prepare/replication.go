package prepare

import (
	"antdb/config"
	"antdb/internal/network"
	"antdb/internal/service/storage/replication"
	"antdb/internal/service/storage/wal"
	"go.uber.org/zap"
)

const (
	maxMasterConnections = 5
	messageSize          = 10 << 20
)

func CreateMasterReplication(
	replicationCfg *config.ReplicationConfig,
	walCfg *config.WALConfig,
	log *zap.Logger,
) (*replication.Master, error) {
	replicaServer, err := network.NewServer(
		replicationCfg.MasterAddress,
		maxMasterConnections,
		messageSize,
		log)
	if err != nil {
		log.Fatal("can't create replica server", zap.Error(err))
	}
	return replication.NewMaster(replicaServer, walCfg.DataDirectory, log), nil
}

func CreateSlaveReplication(
	replicationCfg *config.ReplicationConfig,
	walCfg *config.WALConfig,
	streamCh chan []*wal.Unit,
	log *zap.Logger,
) (*replication.Slave, error) {
	return replication.NewSlave(
		replicationCfg.MasterAddress,
		replicationCfg.SyncInterval,
		walCfg.DataDirectory,
		streamCh,
		log)
}
