package scas

import (
  "log"
)

// values in memory that have been tx logged, but not yet written to an SSTable
type memTable struct {
	store map[Key] Value
	maxSize   int // max keys before table is flushed to SSTable
	maxTime   int // max seconds before table is flushed to SSTable
	lastFlush int // last time table was flushed
}

func newMemTable() memTable {
  return memTable{store: make(map[Key] Value), maxSize: 128, maxTime: 2^31}
}

func (m memTable) has(k *Key) (ok bool) {
  _, ok = m.store[*k]
  return
}

func (m memTable) get(k *Key) (v Value) {
  v, _ = m.store[*k]
  return
}

func (m memTable) put(k *Key, v Value) error {
  log.Println("memtable: put key / value", k, v)
  m.store[*k]=v
  return nil
}

func (m memTable) flush() (*ssTable, error) {
  if len(m.store) > 0 {
    return writeSSTable(m)
  }
  return nil,nil
}
