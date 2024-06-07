package replication

import "context"

type Replication interface {
	Start(context.Context)
	Stop(context.Context)
	IsMaster() bool
}
