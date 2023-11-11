package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, _ := server.storage.Reader(nil)
	defer reader.Close()
	val, err := reader.GetCF(req.Cf, req.Key)
	if val == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: val}, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}
	batch := []storage.Modify{{Data: put}}
	err := server.storage.Write(nil, batch)
	return nil, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	del := storage.Delete{Key: req.Key, Cf: req.Cf}
	batch := []storage.Modify{{Data: del}}
	err := server.storage.Write(nil, batch)
	return nil, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, _ := server.storage.Reader(nil)
	defer reader.Close()
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	response := kvrpcpb.RawScanResponse{}
	var fetched uint32 = 0
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {

		// if !bytes.Equal(iter.Item().Key(), req.StartKey) {
		// 	continue
		// }
		fetched++
		k := iter.Item().Key()
		v, _ := iter.Item().Value()
		response.Kvs = append(response.Kvs, &kvrpcpb.KvPair{Key: k, Value: v})
		if fetched == req.Limit {
			break
		}
	}
	return &response, nil
}
