package wal

import "antdb/internal/service/compute"

type Unit struct {
	Command   string
	Arguments []string
}

type UnitData struct {
	Unit    *Unit
	ErrChan chan error
}

func NewUnit(command compute.Command, arguments []string) *Unit {
	return &Unit{
		Command:   string(command),
		Arguments: arguments,
	}
}
