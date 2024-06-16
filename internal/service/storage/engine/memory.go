package engine

import (
	"sync"
)

type MemoryTable struct {
	mutex sync.RWMutex
	data  map[string]string
}

func NewMemoryTable() *MemoryTable {
	return &MemoryTable{
		data: make(map[string]string),
	}
}

func (s *MemoryTable) Set(key, value string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.data[key] = value
}

func (s *MemoryTable) Get(key string) (string, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	value, found := s.data[key]
	return value, found
}

func (s *MemoryTable) Del(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.data, key)
}

func (s *MemoryTable) Export() map[string]string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	copyData := make(map[string]string, len(s.data))

	for key, value := range s.data {
		copyData[key] = value
	}

	return copyData
}
