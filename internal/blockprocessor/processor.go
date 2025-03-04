// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockprocessor

import (
	"sync"

	"github.com/IBM-Blockchain/bcdb-server/internal/blockstore"
	"github.com/IBM-Blockchain/bcdb-server/internal/mptrie"
	"github.com/IBM-Blockchain/bcdb-server/internal/mtree"
	"github.com/IBM-Blockchain/bcdb-server/internal/provenance"
	"github.com/IBM-Blockchain/bcdb-server/internal/queue"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

// BlockProcessor holds block validator and committer
type BlockProcessor struct {
	blockOneQueueBarrier *queue.OneQueueBarrier
	blockStore           *blockstore.Store
	validator            *validator
	committer            *committer
	listeners            *blockCommitListeners
	started              chan struct{}
	stop                 chan struct{}
	stopped              chan struct{}
	logger               *logger.SugarLogger
}

// Config holds the configuration information needed to bootstrap the
// block processor
type Config struct {
	BlockOneQueueBarrier *queue.OneQueueBarrier
	BlockStore           *blockstore.Store
	DB                   worldstate.DB
	ProvenanceStore      *provenance.Store
	StateTrieStore       mptrie.Store
	Logger               *logger.SugarLogger
}

// New creates a ValidatorAndCommitter
func New(conf *Config) *BlockProcessor {
	return &BlockProcessor{
		blockOneQueueBarrier: conf.BlockOneQueueBarrier,
		blockStore:           conf.BlockStore,
		validator:            newValidator(conf),
		committer:            newCommitter(conf),
		listeners:            newBlockCommitListeners(conf.Logger),
		started:              make(chan struct{}),
		stop:                 make(chan struct{}),
		stopped:              make(chan struct{}),
		logger:               conf.Logger,
	}
}

// Start starts the validator and committer
func (b *BlockProcessor) Start() {
	b.logger.Debug("starting the block processor")
	defer close(b.stopped)

	if err := b.recoverWorldStateDBIfNeeded(); err != nil {
		panic(errors.WithMessage(err, "error while recovering node"))
	}

	if err := b.initAndRecoverStateTrieIfNeeded(); err != nil {
		panic(errors.WithMessage(err, "error while recovering node state trie"))
	}

	b.logger.Debug("block processor has been started successfully")
	close(b.started)
	for {
		select {
		case <-b.stop:
			b.logger.Info("stopping block processing")
			return

		default:
			// The replication layer go-routine that enqueued the block will be blocked until after commit; it must be
			// released by calling OneQueueBarrier.Reply().
			blockData, err := b.blockOneQueueBarrier.Dequeue()
			if err != nil {
				// when the queue is closed during the teardown/cleanup
				b.logger.Debugf("OneQueueBarrier error: %s", err)
				continue
			}
			block := blockData.(*types.Block)

			b.logger.Debugf("validating and committing block %d", block.GetHeader().GetBaseHeader().GetNumber())
			validationInfo, err := b.validator.validateBlock(block)
			if err != nil {
				panic(err)
			}

			block.Header.ValidationInfo = validationInfo

			if err := b.blockStore.AddSkipListLinks(block); err != nil {
				panic(err)
			}

			root, err := mtree.BuildTreeForBlockTx(block)
			if err != nil {
				panic(err)
			}
			block.Header.TxMerkelTreeRootHash = root.Hash()

			if err = b.committer.commitBlock(block); err != nil {
				panic(err)
			}

			b.logger.Debugf("validated and committed block %d\n", block.GetHeader().GetBaseHeader().GetNumber())

			//TODO detect config changes that affect the replication component and return an appropriate non-nil object
			// to instruct it to reconfigure itself. See issues:
			// https://github.ibm.com/blockchaindb/server/issues/396
			// https://github.ibm.com/blockchaindb/server/issues/413
			var reConfig interface{}

			// The replication layer go-routine is blocked until after commit, and is released by calling Reply().
			// The post-commit listeners are called after the replication layer go-routine is released. This is an
			// optimization, as post-commit processing can proceed in parallel with the replication go-routine handling
			// the next block.
			err = b.blockOneQueueBarrier.Reply(reConfig)
			if err != nil {
				// when the queue is closed during the teardown/cleanup
				b.logger.Debugf("OneQueueBarrier error: %s", err)
				continue
			}

			if err = b.listeners.invoke(block); err != nil {
				panic(err)
			}
		}
	}
}

// WaitTillStart waits till the block processor is started
func (b *BlockProcessor) WaitTillStart() {
	<-b.started
}

// Stop stops the block processor
func (b *BlockProcessor) Stop() {
	if err := b.blockOneQueueBarrier.Close(); err != nil {
		b.logger.Warnf("OneQueueBarrier error: %s", err)
	}
	close(b.stop)
	<-b.stopped
}

func (b *BlockProcessor) recoverWorldStateDBIfNeeded() error {
	blockStoreHeight, err := b.blockStore.Height()
	if err != nil {
		return err
	}

	stateDBHeight, err := b.committer.db.Height()
	if err != nil {
		return err
	}

	switch {
	case stateDBHeight == blockStoreHeight:
		return nil
	case stateDBHeight > blockStoreHeight:
		return errors.Errorf(
			"the height of state database [%d] is higher than the height of block store [%d]. The node cannot be recovered",
			stateDBHeight,
			blockStoreHeight,
		)
	case blockStoreHeight-stateDBHeight > 1:
		// Note: when we support rollback, the different in height can be more than 1.
		// For now, a failure can occur before committing the block to the block store or after.
		// As a result, the height of block store would be at most 1 higher than the state database
		// height.
		return errors.Errorf(
			"the difference between the height of the block store [%d] and the state database [%d] cannot be greater than 1 block. The node cannot be recovered",
			blockStoreHeight,
			stateDBHeight,
		)
	case blockStoreHeight-stateDBHeight == 1:
		block, err := b.blockStore.Get(blockStoreHeight)
		if err != nil {
			return err
		}
		dbsUpdates, provenanceData, err := b.committer.constructDBAndProvenanceEntries(block)
		if err != nil {
			return err
		}
		return b.committer.commitToDBs(dbsUpdates, provenanceData, block)
	}

	return nil
}

func (b *BlockProcessor) initAndRecoverStateTrieIfNeeded() error {
	trieStoreHeight, blockStoreHeight, stateTrie, err := loadStateTrie(b.committer.stateTrieStore, b.blockStore)
	if err != nil {
		return err
	}
	b.committer.stateTrie = stateTrie
	if blockStoreHeight != trieStoreHeight {
		switch {
		case trieStoreHeight+1 == blockStoreHeight:
			b.logger.Warnf("state trie store not updated, last block in block store is %d, last block in state trie is %d, updating trie", blockStoreHeight, trieStoreHeight)
			block, err := b.blockStore.Get(blockStoreHeight)
			if err != nil {
				return err
			}
			dbsUpdates, _, err := b.committer.constructDBAndProvenanceEntries(block)
			if err != nil {
				return err
			}
			if err = b.committer.applyBlockOnStateTrie(dbsUpdates); err != nil {
				return err
			}
			if err = b.committer.commitTrie(blockStoreHeight); err != nil {
				return err
			}
		default:
			return errors.Errorf(
				"the difference between the height of the block store [%d] and the state trie store [%d] cannot be greater than 1 block. The node cannot be recovered",
				blockStoreHeight,
				trieStoreHeight,
			)
		}
	}
	return nil
}

// RegisterBlockCommitListener registers a commit listener with the block processor
func (b *BlockProcessor) RegisterBlockCommitListener(name string, listener BlockCommitListener) error {
	return b.listeners.add(name, listener)
}

type blockCommitListeners struct {
	listens map[string]BlockCommitListener
	logger  *logger.SugarLogger
	sync.RWMutex
}

func newBlockCommitListeners(logger *logger.SugarLogger) *blockCommitListeners {
	return &blockCommitListeners{
		listens: make(map[string]BlockCommitListener),
		logger:  logger,
	}
}

//go:generate mockery --dir . --name BlockCommitListener --case underscore --output mocks/

// BlockCommitListener is a listener who listens to the
// commit events
type BlockCommitListener interface {
	PostBlockCommitProcessing(block *types.Block) error
}

func (l *blockCommitListeners) add(name string, listener BlockCommitListener) error {
	l.Lock()
	defer l.Unlock()

	l.logger.Info("Registering listener [" + name + "]")
	if _, ok := l.listens[name]; ok {
		return errors.Errorf("the listener [" + name + "] is already registered")
	}

	l.listens[name] = listener
	return nil
}

func (l *blockCommitListeners) invoke(block *types.Block) error {
	l.RLock()
	defer l.RUnlock()

	for name, listener := range l.listens {
		l.logger.Debugf("Invoking listener [%s] for block [%d]", name, block.Header.BaseHeader.Number)
		if err := listener.PostBlockCommitProcessing(block); err != nil {
			return errors.WithMessage(err, "error while invoking listener ["+name+"]")
		}
	}

	return nil
}

func loadStateTrie(mpTrieStore mptrie.Store, blockStore *blockstore.Store) (uint64, uint64, *mptrie.MPTrie, error) {
	blockStoreHeight, err := blockStore.Height()
	if err != nil {
		return 0, 0, nil, err
	}

	if blockStoreHeight == 0 {
		trie, err := mptrie.NewTrie(nil, mpTrieStore)
		return 0, 0, trie, err
	}

	trieStoreHeight, err := mpTrieStore.Height()
	if err == leveldb.ErrNotFound {
		trie, err := mptrie.NewTrie(nil, mpTrieStore)
		return 0, blockStoreHeight, trie, err
	}

	height := blockStoreHeight
	if trieStoreHeight < height {
		height = trieStoreHeight
	} else if trieStoreHeight > height {
		// Impossible situation, because commit to block store executed before commit to trie store
		// this indicate problem with underline database
		return trieStoreHeight, blockStoreHeight, nil, errors.Errorf("height of block store (%d) is smalled that height of trie store (%d), check underline database", blockStoreHeight, trieStoreHeight)
	}

	lastTrieBlockHeader, err := blockStore.GetHeader(height)
	if err != nil {
		return 0, blockStoreHeight, nil, err
	}

	trie, err := mptrie.NewTrie(lastTrieBlockHeader.GetStateMerkelTreeRootHash(), mpTrieStore)
	return height, blockStoreHeight, trie, err
}
