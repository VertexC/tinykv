package standalone_storage

import (
	"path/filepath"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	Engines *engine_util.Engines
}

type StandAloneStorageReader struct {
	Txn       *badger.Txn
	Iterators []engine_util.DBIterator
}

func (reader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(reader.Txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (reader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	iterator := engine_util.NewCFIterator(cf, reader.Txn)
	reader.Iterators = append(reader.Iterators, iterator)
	return iterator
}

func (reader *StandAloneStorageReader) Close() {
	reader.Txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// config badger db option

	kvEngine := engine_util.CreateDB(conf.DBPath, false)
	engines := engine_util.NewEngines(kvEngine, nil, filepath.Join(conf.DBPath, "kv"), "")
	storage := StandAloneStorage{Engines: engines}

	return &storage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.Engines.Kv.Close()
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.Engines.Kv.NewTransaction(false)
	reader := StandAloneStorageReader{Txn: txn}
	return &reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			if err := engine_util.PutCF(s.Engines.Kv, modify.Cf(), modify.Key(), modify.Value()); err != nil {
				return nil
			}
		case storage.Delete:
			if err := engine_util.DeleteCF(s.Engines.Kv, modify.Cf(), modify.Key()); err != nil {
				return nil
			}
		}
	}
	return nil
}
