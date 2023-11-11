package standalone_storage

import (
	"fmt"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	conf *config.Config
	db   *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	// no raft for project1, set to false
	//db := engine_util.CreateDB(conf.DBPath, false)
	return &StandAloneStorage{conf: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	dir := s.conf.DBPath
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	s.db = db
	if db == nil {
		fmt.Println("db == nil")
	}
	return err
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.db.Close()
	return err
}

// reader for standalone storage
type standAloneReader struct {
	inner     *StandAloneStorage
	iterCount int
	txn       *badger.Txn
}

// Close implements storage.StorageReader.
func (sar *standAloneReader) Close() {
	sar.txn.Discard()
}

// IterCF implements storage.StorageReader.
func (sar *standAloneReader) IterCF(cf string) engine_util.DBIterator {
	// read-only txn
	txn := sar.inner.db.NewTransaction(false)
	iter := engine_util.NewCFIterator(cf, txn)
	return iter
}

// GetCF implements storage.StorageReader.
func (sar *standAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	// read-only txn
	sar.txn = sar.inner.db.NewTransaction(false)
	//defer txn.Discard()
	val, _ := engine_util.GetCF(sar.inner.db, cf, key)
	return val, nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	return &standAloneReader{s, 0, txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// use badger transaction
	txn := s.db.NewTransaction(true)
	defer txn.Commit()
	write_batch := new(engine_util.WriteBatch)
	for _, entry := range batch {
		// handle Put and Delete separtely
		switch entry.Data.(type) {
		case storage.Put:
			write_batch.SetCF(entry.Cf(), entry.Key(), entry.Value())
		case storage.Delete:
			write_batch.DeleteCF(entry.Cf(), entry.Key())
		}
	}
	err := write_batch.WriteToDB(s.db)
	return err
}
