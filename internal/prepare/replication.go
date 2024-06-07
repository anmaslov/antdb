package prepare

import (
	"antdb/config"
	"antdb/internal/network"
	"antdb/internal/service/storage/replication"
	"context"
	"fmt"
	"go.uber.org/zap"
)

func CreateMasterReplication(
	ctx context.Context,
	cfg *config.ReplicationConfig,
	log *zap.Logger,
) (*replication.Master, error) {
	replicaServer, err := network.NewServer(
		cfg.MasterAddress,
		5,
		10<<20,
		log)
	if err != nil {
		log.Fatal("can't create replica server", zap.Error(err))
	}
	replicaMaster := replication.NewMaster(replicaServer)
	err = replicaMaster.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("can't start replica server: %w", err)
	}

	return replicaMaster, nil
}

func CreateSlaveReplication(
	ctx context.Context,
	cfg *config.ReplicationConfig,
	log *zap.Logger,
) (*replication.Slave, error) {
	replicationClient, err := replication.NewSlave(cfg.MasterAddress, cfg.SyncInterval, "dir", log)
	if err != nil {
		return nil, fmt.Errorf("can't create replication client: %w", err)
	}

	replicationClient.Start(ctx)

	return nil, nil
}
