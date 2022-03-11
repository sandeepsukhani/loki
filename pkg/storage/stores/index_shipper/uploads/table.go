package uploads

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/loki/pkg/storage/stores/index_shipper/index"
	"github.com/grafana/loki/pkg/storage/stores/shipper/storage"
)

const (
	// a temp file is created during uploads with name of the db + tempFileSuffix
	tempFileSuffix = ".temp"
)

type Table interface {
	Name() string
	AddIndex(userID string, idx index.Index) error
	ForEach(userID string, callback func(index index.Index) error) error
	Upload(ctx context.Context) error
	Cleanup(indexRetainPeriod time.Duration) error
	Stop()
}

// table is a collection of multiple dbs created for a same table by the ingester.
// All the public methods are concurrency safe and take care of mutexes to avoid any data race.
type table struct {
	name                                 string
	uploader                             string
	baseUserIndexSet, baseCommonIndexSet storage.IndexSet
	logger                               log.Logger

	indexSet    map[string]IndexSet
	indexSetMtx sync.RWMutex
}

// NewTable create a new table without looking for any existing local dbs belonging to the table.
func NewTable(name, uploader string, storageClient storage.Client) Table {
	return &table{
		name:               name,
		uploader:           uploader,
		baseUserIndexSet:   storage.NewIndexSet(storageClient, true),
		baseCommonIndexSet: storage.NewIndexSet(storageClient, false),
		indexSet:           make(map[string]IndexSet),
	}
}

func (lt *table) Name() string {
	return lt.name
}

func (lt *table) AddIndex(userID string, idx index.Index) error {
	lt.indexSetMtx.Lock()
	defer lt.indexSetMtx.Unlock()

	if _, ok := lt.indexSet[userID]; !ok {
		baseIndexSet := lt.baseUserIndexSet
		if userID == "" {
			baseIndexSet = lt.baseCommonIndexSet
		}
		idxSet, err := NewIndexSet(lt.name, userID, baseIndexSet, lt.logger)
		if err != nil {
			return err
		}

		lt.indexSet[userID] = idxSet
	}

	lt.indexSet[userID].Add(idx)
	return nil
}

func (lt *table) ForEach(userID string, callback func(index index.Index) error) error {
	lt.indexSetMtx.RLock()
	defer lt.indexSetMtx.RUnlock()

	idxSet, ok := lt.indexSet[userID]
	if !ok {
		return nil
	}

	return idxSet.ForEach(callback)
}

func (lt *table) Upload(ctx context.Context) error {
	lt.indexSetMtx.RLock()
	defer lt.indexSetMtx.RUnlock()

	for _, indexSet := range lt.indexSet {
		if err := indexSet.Upload(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (lt *table) Cleanup(indexRetainPeriod time.Duration) error {
	lt.indexSetMtx.RLock()
	defer lt.indexSetMtx.RUnlock()

	for _, indexSet := range lt.indexSet {
		if err := indexSet.Cleanup(indexRetainPeriod); err != nil {
			return err
		}
	}

	return nil
}

// Stop closes all the open dbs.
func (lt *table) Stop() {
	lt.indexSetMtx.Lock()
	defer lt.indexSetMtx.Unlock()

	for _, indexSet := range lt.indexSet {
		indexSet.Close()
	}
}
