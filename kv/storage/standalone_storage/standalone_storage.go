package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	Kv     *badger.DB
	KvPath string
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	kv := engine_util.CreateDB(conf.DBPath, false)
	return &StandAloneStorage{
		Kv:     kv,
		KvPath: conf.DBPath,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	if err := s.Kv.Close(); err != nil {
		return err
	}

	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{
		storage: s,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, v := range batch {
		switch v.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.Kv, v.Cf(), v.Key(), v.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.Kv, v.Cf(), v.Key())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type StandAloneStorageReader struct {
	storage *StandAloneStorage
}

func (reader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, _ := engine_util.GetCF(reader.storage.Kv, cf, key)
	return val, nil
}

func (reader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := reader.storage.Kv.NewTransaction(false)
	//defer txn.Discard()
	return engine_util.NewCFIterator(cf, txn)
}

func (reader *StandAloneStorageReader) Close() {
	reader.storage.Kv.Close()
}
