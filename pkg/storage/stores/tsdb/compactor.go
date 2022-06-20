package tsdb

import (
	"context"
	"math"
	"os"
	"time"
	"unsafe"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/pkg/logproto"
	"github.com/grafana/loki/pkg/storage/chunk"
	"github.com/grafana/loki/pkg/storage/config"
	index_shipper "github.com/grafana/loki/pkg/storage/stores/indexshipper/index"
	"github.com/grafana/loki/pkg/storage/stores/shipper/compactor"
	"github.com/grafana/loki/pkg/storage/stores/shipper/compactor/retention"
	"github.com/grafana/loki/pkg/storage/stores/tsdb/index"
)

type indexProcessor struct{}

func NewIndexCompactor() compactor.IndexCompactor {
	return indexProcessor{}
}

func (i indexProcessor) NewTableCompactor(ctx context.Context, commonIndexSet compactor.IndexSet, existingUserIndexSet map[string]compactor.IndexSet, userIndexSetFactoryFunc compactor.MakeEmptyUserIndexSetFunc, periodConfig config.PeriodConfig) compactor.TableCompactor {
	return newTableCompactor(ctx, commonIndexSet, existingUserIndexSet, userIndexSetFactoryFunc, periodConfig)
}

func (i indexProcessor) OpenCompactedIndexFile(ctx context.Context, path, tableName, userID, workingDir string, periodConfig config.PeriodConfig, logger log.Logger) (compactor.CompactedIndex, error) {
	indexFile, err := OpenShippableTSDB(path)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := indexFile.Close(); err != nil {
			level.Error(logger).Log("msg", "failed to close index file", "err", err)
		}
	}()

	builder := NewBuilder()
	err = indexFile.(*TSDBFile).Index.(*TSDBIndex).forSeries(ctx, nil, func(lbls labels.Labels, fp model.Fingerprint, chks []index.ChunkMeta) {
		builder.AddSeries(lbls.Copy(), fp, chks)
	}, labels.MustNewMatcher(labels.MatchEqual, "", ""))
	if err != nil {
		return nil, err
	}

	builder.chunksFinalized = true

	return newUserIndex(ctx, tableName, userID, workingDir, periodConfig, builder), nil
}

type tableCompactor struct {
	commonIndexSet          compactor.IndexSet
	existingUserIndexSet    map[string]compactor.IndexSet
	userIndexSetFactoryFunc compactor.MakeEmptyUserIndexSetFunc
	ctx                     context.Context
	periodConfig            config.PeriodConfig
}

func newTableCompactor(
	ctx context.Context,
	commonIndexSet compactor.IndexSet,
	existingUserIndexSet map[string]compactor.IndexSet,
	userIndexSetFactoryFunc compactor.MakeEmptyUserIndexSetFunc,
	periodConfig config.PeriodConfig,
) *tableCompactor {
	return &tableCompactor{
		ctx:                     ctx,
		commonIndexSet:          commonIndexSet,
		existingUserIndexSet:    existingUserIndexSet,
		userIndexSetFactoryFunc: userIndexSetFactoryFunc,
		periodConfig:            periodConfig,
	}
}

func (t *tableCompactor) CompactTable() error {
	multiTenantIndexes := t.commonIndexSet.ListSourceFiles()

	var indices []Index

	for _, indexFile := range multiTenantIndexes {
		downloadedAt, err := t.commonIndexSet.GetSourceFile(indexFile)
		if err != nil {
			return err
		}

		idx, err := OpenShippableTSDB(downloadedAt)
		if err != nil {
			return err
		}

		indices = append(indices, idx.(Index))
	}

	var multiTenantIndex Index = NoopIndex{}
	if len(indices) > 0 {
		var err error
		multiTenantIndex, err = NewMultiIndex(indices...)
		if err != nil {
			return err
		}
	}

	userIDs, err := multiTenantIndex.LabelValues(t.ctx, "", 0, math.MaxInt64, TenantLabel)
	if err != nil {
		return err
	}

	compactedIndexes := make(map[string]compactor.CompactedIndex, len(userIDs))
	for _, userID := range userIDs {
		existingUserIndexSet, ok := t.existingUserIndexSet[userID]
		if !ok {
			var err error
			existingUserIndexSet, err = t.userIndexSetFactoryFunc(userID)
			if err != nil {
				return err
			}
		}

		builder, err := setupBuilder(t.ctx, userID, existingUserIndexSet, indices)
		if err != nil {
			return err
		}

		compactedIndex := newUserIndex(t.ctx, existingUserIndexSet.GetTableName(), userID, existingUserIndexSet.GetWorkingDir(), t.periodConfig, builder)
		compactedIndexes[userID] = compactedIndex

		if err := existingUserIndexSet.SetCompactedIndex(compactedIndex, true); err != nil {
			return err
		}
	}

	for userID, srcIdxSet := range t.existingUserIndexSet {
		if _, ok := compactedIndexes[userID]; ok || len(srcIdxSet.ListSourceFiles()) <= 1 {
			continue
		}

		builder, err := setupBuilder(t.ctx, userID, srcIdxSet, indices)
		if err != nil {
			return err
		}

		compactedIndex := newUserIndex(t.ctx, srcIdxSet.GetTableName(), userID, srcIdxSet.GetWorkingDir(), t.periodConfig, builder)
		compactedIndexes[userID] = compactedIndex
		if err := srcIdxSet.SetCompactedIndex(compactedIndex, true); err != nil {
			return err
		}
	}

	if len(indices) > 0 {
		if err := t.commonIndexSet.SetCompactedIndex(nil, true); err != nil {
			return err
		}
	}
	return err
}

func setupBuilder(ctx context.Context, userID string, sourceIndexSet compactor.IndexSet, multiTenantIndexes []Index) (*Builder, error) {
	sourceIndexes := sourceIndexSet.ListSourceFiles()
	builder := NewBuilder()

	for _, idx := range multiTenantIndexes {
		err := idx.(*TSDBFile).Index.(*TSDBIndex).forSeries(ctx, nil, func(lbls labels.Labels, fp model.Fingerprint, chks []index.ChunkMeta) {
			builder.AddSeries(withoutTenantLabel(lbls.Copy()), fp, chks)
		}, withTenantLabelMatcher(userID, []*labels.Matcher{})...)
		if err != nil {
			return nil, err
		}
	}

	for _, sourceIndex := range sourceIndexes {
		path, err := sourceIndexSet.GetSourceFile(sourceIndex)
		if err != nil {
			return nil, err
		}

		defer func() {
			if err := os.Remove(path); err != nil {
				level.Error(sourceIndexSet.GetLogger()).Log("msg", "error removing source index file", "err", err)
			}
		}()

		indexFile, err := OpenShippableTSDB(path)
		if err != nil {
			return nil, err
		}

		defer func() {
			if err := indexFile.Close(); err != nil {
				level.Error(sourceIndexSet.GetLogger()).Log("msg", "failed to close index file", "err", err)
			}
		}()

		err = indexFile.(*TSDBFile).Index.(*TSDBIndex).forSeries(ctx, nil, func(lbls labels.Labels, fp model.Fingerprint, chks []index.ChunkMeta) {
			builder.AddSeries(lbls.Copy(), fp, chks)
		}, labels.MustNewMatcher(labels.MatchEqual, "", ""))
		if err != nil {
			return nil, err
		}
	}

	builder.FinalizeChunks()

	return builder, nil
}

type compactedIndex struct {
	ctx           context.Context
	userID        string
	builder       *Builder
	workingDir    string
	tableInterval model.Interval
	periodConfig  config.PeriodConfig

	indexChunks     []chunk.Chunk
	deleteChunks    map[string][]index.ChunkMeta
	seriesToCleanup map[string]struct{}
}

func newUserIndex(ctx context.Context, tableName, userID, workingDir string, periodConfig config.PeriodConfig, builder *Builder) *compactedIndex {
	return &compactedIndex{
		ctx:             ctx,
		userID:          userID,
		builder:         builder,
		workingDir:      workingDir,
		periodConfig:    periodConfig,
		tableInterval:   retention.ExtractIntervalFromTableName(tableName),
		deleteChunks:    map[string][]index.ChunkMeta{},
		seriesToCleanup: map[string]struct{}{},
	}
}

func (c *compactedIndex) ForEachChunk(callback retention.ChunkEntryCallback) error {
	schemaCfg := config.SchemaConfig{
		Configs: []config.PeriodConfig{c.periodConfig},
	}

	chunkEntry := retention.ChunkEntry{
		ChunkRef: retention.ChunkRef{
			UserID: getUnsafeBytes(c.userID),
		},
	}
	logprotoChunkRef := logproto.ChunkRef{
		UserID: c.userID,
	}
	for seriesID, stream := range c.builder.streams {
		logprotoChunkRef.Fingerprint = uint64(stream.fp)
		chunkEntry.SeriesID = getUnsafeBytes(seriesID)
		chunkEntry.Labels = withoutTenantLabel(stream.labels)

		for _, chk := range stream.chunks {
			logprotoChunkRef.From = chk.From()
			logprotoChunkRef.Through = chk.Through()
			logprotoChunkRef.Checksum = chk.Checksum

			chunkEntry.ChunkID = getUnsafeBytes(schemaCfg.ExternalKey(logprotoChunkRef))
			chunkEntry.From = logprotoChunkRef.From
			chunkEntry.Through = logprotoChunkRef.Through

			deleteChunk, err := callback(chunkEntry)
			if err != nil {
				return err
			}

			if deleteChunk {
				c.deleteChunks[seriesID] = append(c.deleteChunks[seriesID], chk)
			}
		}
	}

	return nil
}

func (c *compactedIndex) IndexChunk(chk chunk.Chunk) (bool, error) {
	if chk.From > c.tableInterval.End || c.tableInterval.Start > chk.Through {
		return false, nil
	}

	c.indexChunks = append(c.indexChunks, chk)

	return true, nil
}

func (c *compactedIndex) CleanupSeries(_ []byte, lbls labels.Labels) error {
	seriesID := lbls.String()
	delete(c.builder.streams, seriesID)
	delete(c.deleteChunks, seriesID)
	return nil
}

func (c *compactedIndex) Cleanup() {}

func (c *compactedIndex) ToIndexFile() (index_shipper.Index, error) {
	for seriesID, chks := range c.deleteChunks {
		for _, chk := range chks {
			if err := c.builder.DropChunk(seriesID, chk); err != nil {
				return nil, err
			}
		}
	}
	c.deleteChunks = nil

	for _, chk := range c.indexChunks {
		err := c.builder.InsertChunk(chk.Metric.String(), index.ChunkMeta{
			Checksum: chk.Checksum,
			MinTime:  int64(chk.From),
			MaxTime:  int64(chk.Through),
			KB:       uint32(chk.Size()) / (1 << 10),
			Entries:  uint32(chk.Data.Entries()),
		})
		if err != nil {
			return nil, err
		}
	}
	c.indexChunks = nil

	id, err := c.builder.Build(c.ctx, c.workingDir, func(from, through model.Time, checksum uint32) Identifier {
		id := SingleTenantTSDBIdentifier{
			TS:       time.Now(),
			From:     from,
			Through:  through,
			Checksum: checksum,
		}
		return newPrefixedIdentifier(id, c.workingDir, "")
	})
	if err != nil {
		return nil, err
	}

	return NewShippableTSDBFile(id, false)
}

func getUnsafeBytes(s string) []byte {
	return *((*[]byte)(unsafe.Pointer(&s)))
}
