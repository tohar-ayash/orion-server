// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io/ioutil"
	"time"

	"github.com/IBM-Blockchain/bcdb-server/config"
	"github.com/IBM-Blockchain/bcdb-server/internal/blockstore"
	"github.com/IBM-Blockchain/bcdb-server/internal/fileops"
	"github.com/IBM-Blockchain/bcdb-server/internal/identity"
	mptrieStore "github.com/IBM-Blockchain/bcdb-server/internal/mptrie/store"
	"github.com/IBM-Blockchain/bcdb-server/internal/provenance"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate/leveldb"
	"github.com/IBM-Blockchain/bcdb-server/pkg/crypto"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

//go:generate mockery --dir . --name DB --case underscore --output mocks/

// DB encapsulates functionality required to operate with database state
type DB interface {
	// LedgerHeight returns current height of the ledger
	LedgerHeight() (uint64, error)

	// Height returns ledger height
	Height() (uint64, error)

	// DoesUserExist checks whenever user with given userID exists
	DoesUserExist(userID string) (bool, error)

	// GetCertificate returns the certificate associated with useID, if it exists.
	GetCertificate(userID string) (*x509.Certificate, error)

	// GetUser retrieves user' record
	GetUser(querierUserID, targetUserID string) (*types.ResponseEnvelope, error)

	// GetConfig returns database configuration
	GetConfig() (*types.ResponseEnvelope, error)

	// GetNodeConfig returns single node subsection of database configuration
	GetNodeConfig(nodeID string) (*types.ResponseEnvelope, error)

	// GetDBStatus returns status for database, checks whenever database was created
	GetDBStatus(dbName string) (*types.ResponseEnvelope, error)

	// GetData retrieves values for given key
	GetData(dbName, querierUserID, key string) (*types.ResponseEnvelope, error)

	// GetBlockHeader returns ledger block header
	GetBlockHeader(userID string, blockNum uint64) (*types.ResponseEnvelope, error)

	// GetTxProof returns intermediate hashes to recalculate merkle tree root from tx hash
	GetTxProof(userID string, blockNum uint64, txIdx uint64) (*types.ResponseEnvelope, error)

	// GetLedgerPath returns list of blocks that forms shortest path in skip list chain in ledger
	GetLedgerPath(userID string, start, end uint64) (*types.ResponseEnvelope, error)

	// GetValues returns all values associated with a given key
	GetValues(dbName, key string) (*types.ResponseEnvelope, error)

	// GetDeletedValues returns all deleted values associated with a given key
	GetDeletedValues(dbname, key string) (*types.ResponseEnvelope, error)

	// GetValueAt returns the value of a given key at a particular version
	GetValueAt(dbName, key string, version *types.Version) (*types.ResponseEnvelope, error)

	// GetMostRecentValueAtOrBelow returns the most recent value of a given key at or below the given version
	GetMostRecentValueAtOrBelow(dbName, key string, version *types.Version) (*types.ResponseEnvelope, error)

	// GetPreviousValues returns previous values of a given key and a version. The number of records returned would be limited
	// by the limit parameters.
	GetPreviousValues(dbname, key string, version *types.Version) (*types.ResponseEnvelope, error)

	// GetNextValues returns next values of a given key and a version. The number of records returned would be limited
	// by the limit parameters.
	GetNextValues(dbname, key string, version *types.Version) (*types.ResponseEnvelope, error)

	// GetValuesReadByUser returns all values read by a given user
	GetValuesReadByUser(userID string) (*types.ResponseEnvelope, error)

	// GetValuesReadByUser returns all values read by a given user
	GetValuesWrittenByUser(userID string) (*types.ResponseEnvelope, error)

	// GetValuesDeletedByUser returns all values deleted by a given user
	GetValuesDeletedByUser(userID string) (*types.ResponseEnvelope, error)

	// GetReaders returns all userIDs who have accessed a given key as well as the access frequency
	GetReaders(dbName, key string) (*types.ResponseEnvelope, error)

	// GetReaders returns all userIDs who have accessed a given key as well as the access frequency
	GetWriters(dbName, key string) (*types.ResponseEnvelope, error)

	// GetTxIDsSubmittedByUser returns all ids of all transactions submitted by a given user
	GetTxIDsSubmittedByUser(userID string) (*types.ResponseEnvelope, error)

	// GetTxReceipt returns transaction receipt - block header of ledger block that contains the transaction
	// and transaction index inside the block
	GetTxReceipt(userId string, txID string) (*types.ResponseEnvelope, error)

	// SubmitTransaction submits transaction to the database with a timeout. If the timeout is
	// set to 0, the submission would be treated as async while a non-zero timeout would be
	// treated as a sync submission. When a timeout occurs with the sync submission, a
	// timeout error will be returned
	SubmitTransaction(tx interface{}, timeout time.Duration) (*types.ResponseEnvelope, error)

	// BootstrapDB given bootstrap configuration initialize database by
	// creating required system tables to include database meta data
	BootstrapDB(conf *config.Configurations) (*types.ResponseEnvelope, error)

	// IsDBExists returns true if database with given name is exists otherwise false
	IsDBExists(name string) bool

	// Close frees and closes resources allocated by database instance
	Close() error
}

type db struct {
	nodeID                   string
	worldstateQueryProcessor *worldstateQueryProcessor
	ledgerQueryProcessor     *ledgerQueryProcessor
	provenanceQueryProcessor *provenanceQueryProcessor
	txProcessor              *transactionProcessor
	db                       worldstate.DB
	blockStore               *blockstore.Store
	provenanceStore          *provenance.Store
	stateTrieStore           *mptrieStore.Store
	signer                   crypto.Signer
	logger                   *logger.SugarLogger
}

// NewDB creates a new database bcdb which handles both the queries and transactions.
func NewDB(localConf *config.LocalConfiguration, logger *logger.SugarLogger) (DB, error) {
	if localConf.Server.Database.Name != "leveldb" {
		return nil, errors.New("only leveldb is supported as the state database")
	}

	ledgerDir := localConf.Server.Database.LedgerDirectory
	if err := createLedgerDir(ledgerDir); err != nil {
		return nil, err
	}

	levelDB, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: constructWorldStatePath(ledgerDir),
			Logger:    logger,
		},
	)
	if err != nil {
		return nil, errors.WithMessage(err, "error while creating the world state database")
	}

	blockStore, err := blockstore.Open(
		&blockstore.Config{
			StoreDir: constructBlockStorePath(ledgerDir),
			Logger:   logger,
		},
	)
	if err != nil {
		return nil, errors.WithMessage(err, "error while creating the block store")
	}

	provenanceStore, err := provenance.Open(
		&provenance.Config{
			StoreDir: constructProvenanceStorePath(ledgerDir),
			Logger:   logger,
		},
	)
	if err != nil {
		return nil, errors.WithMessage(err, "error while creating the block store")
	}

	stateTrieStore, err := mptrieStore.Open(
		&mptrieStore.Config{
			StoreDir: constructStateTrieStorePath(ledgerDir),
			Logger:   logger,
		},
	)
	if err != nil {
		return nil, errors.WithMessage(err, "error while creating the state trie store")
	}

	querier := identity.NewQuerier(levelDB)

	signer, err := crypto.NewSigner(&crypto.SignerOptions{KeyFilePath: localConf.Server.Identity.KeyPath})
	if err != nil {
		return nil, errors.Wrap(err, "can't load private key")
	}

	worldstateQueryProcessor := newWorldstateQueryProcessor(
		&worldstateQueryProcessorConfig{
			nodeID:          localConf.Server.Identity.ID,
			db:              levelDB,
			blockStore:      blockStore,
			identityQuerier: querier,
			logger:          logger,
		},
	)

	ledgerQueryProcessorConfig := &ledgerQueryProcessorConfig{
		db:              levelDB,
		blockStore:      blockStore,
		provenanceStore: provenanceStore,
		identityQuerier: querier,
		logger:          logger,
	}
	ledgerQueryProcessor := newLedgerQueryProcessor(ledgerQueryProcessorConfig)

	provenanceQueryProcessor := newProvenanceQueryProcessor(
		&provenanceQueryProcessorConfig{
			provenanceStore: provenanceStore,
			logger:          logger,
		},
	)

	txProcessor, err := newTransactionProcessor(
		&txProcessorConfig{
			nodeID:             localConf.Server.Identity.ID,
			db:                 levelDB,
			blockStore:         blockStore,
			provenanceStore:    provenanceStore,
			stateTrieStore:     stateTrieStore,
			txQueueLength:      localConf.Server.QueueLength.Transaction,
			txBatchQueueLength: localConf.Server.QueueLength.ReorderedTransactionBatch,
			blockQueueLength:   localConf.Server.QueueLength.Block,
			maxTxCountPerBatch: localConf.BlockCreation.MaxTransactionCountPerBlock,
			batchTimeout:       localConf.BlockCreation.BlockTimeout,
			logger:             logger,
		},
	)
	if err != nil {
		return nil, errors.WithMessage(err, "can't initiate tx processor")
	}

	return &db{
		nodeID:                   localConf.Server.Identity.ID,
		worldstateQueryProcessor: worldstateQueryProcessor,
		ledgerQueryProcessor:     ledgerQueryProcessor,
		provenanceQueryProcessor: provenanceQueryProcessor,
		txProcessor:              txProcessor,
		db:                       levelDB,
		blockStore:               blockStore,
		provenanceStore:          provenanceStore,
		stateTrieStore:           stateTrieStore,
		logger:                   logger,
		signer:                   signer,
	}, nil
}

// BootstrapDB bootstraps DB with system tables
func (d *db) BootstrapDB(conf *config.Configurations) (*types.ResponseEnvelope, error) {
	configTx, err := prepareConfigTx(conf)
	if err != nil {
		return nil, errors.Wrap(err, "failed to prepare and commit a configuration transaction")
	}

	resp, err := d.SubmitTransaction(configTx, 30*time.Second)
	if err != nil {
		return nil, errors.Wrap(err, "error while committing configuration transaction")
	}
	return resp, nil
}

// LedgerHeight returns ledger height
func (d *db) LedgerHeight() (uint64, error) {
	return d.worldstateQueryProcessor.blockStore.Height()
}

// Height returns ledger height
func (d *db) Height() (uint64, error) {
	return d.worldstateQueryProcessor.db.Height()
}

// DoesUserExist checks whenever userID exists
func (d *db) DoesUserExist(userID string) (bool, error) {
	return d.worldstateQueryProcessor.identityQuerier.DoesUserExist(userID)
}

func (d *db) GetCertificate(userID string) (*x509.Certificate, error) {
	return d.worldstateQueryProcessor.identityQuerier.GetCertificate(userID)
}

// GetUser returns user's record
func (d *db) GetUser(querierUserID, targetUserID string) (*types.ResponseEnvelope, error) {
	userResponse, err := d.worldstateQueryProcessor.getUser(querierUserID, targetUserID)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(userResponse)
}

// GetNodeConfig returns single node subsection of database configuration
func (d *db) GetNodeConfig(nodeID string) (*types.ResponseEnvelope, error) {
	nodeConfigResponse, err := d.worldstateQueryProcessor.getNodeConfig(nodeID)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(nodeConfigResponse)
}

// GetConfig returns database configuration
func (d *db) GetConfig() (*types.ResponseEnvelope, error) {
	configResponse, err := d.worldstateQueryProcessor.getConfig()
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(configResponse)
}

// GetDBStatus returns database status
func (d *db) GetDBStatus(dbName string) (*types.ResponseEnvelope, error) {
	dbStatusResponse, err := d.worldstateQueryProcessor.getDBStatus(dbName)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(dbStatusResponse)
}

// SubmitTransaction submits transaction to the database with a timeout. If the timeout is
// set to 0, the submission would be treated as async while a non-zero timeout would be
// treated as a sync submission. When a timeout occurs with the sync submission, a
// timeout error will be returned
func (d *db) SubmitTransaction(tx interface{}, timeout time.Duration) (*types.ResponseEnvelope, error) {
	receipt, err := d.txProcessor.submitTransaction(tx, timeout)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(receipt)
}

// GetData returns value for provided key
func (d *db) GetData(dbName, querierUserID, key string) (*types.ResponseEnvelope, error) {
	dataResponse, err := d.worldstateQueryProcessor.getData(dbName, querierUserID, key)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(dataResponse)
}

func (d *db) IsDBExists(name string) bool {
	return d.worldstateQueryProcessor.isDBExists(name)
}

func (d *db) GetBlockHeader(userID string, blockNum uint64) (*types.ResponseEnvelope, error) {
	blockHeader, err := d.ledgerQueryProcessor.getBlockHeader(userID, blockNum)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(blockHeader)
}

func (d *db) GetTxProof(userID string, blockNum uint64, txIdx uint64) (*types.ResponseEnvelope, error) {
	proofResponse, err := d.ledgerQueryProcessor.getProof(userID, blockNum, txIdx)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(proofResponse)
}

func (d *db) GetLedgerPath(userID string, start, end uint64) (*types.ResponseEnvelope, error) {
	pathResponse, err := d.ledgerQueryProcessor.getPath(userID, start, end)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(pathResponse)
}

func (d *db) GetTxReceipt(userId string, txID string) (*types.ResponseEnvelope, error) {
	receiptResponse, err := d.ledgerQueryProcessor.getTxReceipt(userId, txID)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(receiptResponse)
}

// GetValues returns all values associated with a given key
func (d *db) GetValues(dbName, key string) (*types.ResponseEnvelope, error) {
	values, err := d.provenanceQueryProcessor.GetValues(dbName, key)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(values)
}

// GetDeletedValues returns all deleted values associated with a given key
func (d *db) GetDeletedValues(dbName, key string) (*types.ResponseEnvelope, error) {
	deletedValues, err := d.provenanceQueryProcessor.GetDeletedValues(dbName, key)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(deletedValues)
}

// GetValueAt returns the value of a given key at a particular version
func (d *db) GetValueAt(dbName, key string, version *types.Version) (*types.ResponseEnvelope, error) {
	valueAt, err := d.provenanceQueryProcessor.GetValueAt(dbName, key, version)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(valueAt)
}

// GetMostRecentValueAtOrBelow returns the most recent value of a given key at or below the given version
func (d *db) GetMostRecentValueAtOrBelow(dbName, key string, version *types.Version) (*types.ResponseEnvelope, error) {
	valueAt, err := d.provenanceQueryProcessor.GetMostRecentValueAtOrBelow(dbName, key, version)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(valueAt)
}

// GetPreviousValues returns previous values of a given key and a version. The number of records returned would be limited
// by the limit parameters.
func (d *db) GetPreviousValues(dbName, key string, version *types.Version) (*types.ResponseEnvelope, error) {
	previousValues, err := d.provenanceQueryProcessor.GetPreviousValues(dbName, key, version)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(previousValues)
}

// GetNextValues returns next values of a given key and a version. The number of records returned would be limited
// by the limit parameters.
func (d *db) GetNextValues(dbName, key string, version *types.Version) (*types.ResponseEnvelope, error) {
	nextValues, err := d.provenanceQueryProcessor.GetNextValues(dbName, key, version)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(nextValues)
}

// GetValuesReadByUser returns all values read by a given user
func (d *db) GetValuesReadByUser(userID string) (*types.ResponseEnvelope, error) {
	readByUser, err := d.provenanceQueryProcessor.GetValuesReadByUser(userID)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(readByUser)
}

// GetValuesWrittenByUser returns all values written by a given user
func (d *db) GetValuesWrittenByUser(userID string) (*types.ResponseEnvelope, error) {
	writtenByUser, err := d.provenanceQueryProcessor.GetValuesWrittenByUser(userID)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(writtenByUser)
}

// GetValuesDeletedByUser returns all values deleted by a given user
func (d *db) GetValuesDeletedByUser(userID string) (*types.ResponseEnvelope, error) {
	deletedByUser, err := d.provenanceQueryProcessor.GetValuesDeletedByUser(userID)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(deletedByUser)
}

// GetReaders returns all userIDs who have accessed a given key as well as the access frequency
func (d *db) GetReaders(dbName, key string) (*types.ResponseEnvelope, error) {
	readers, err := d.provenanceQueryProcessor.GetReaders(dbName, key)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(readers)
}

// GetReaders returns all userIDs who have accessed a given key as well as the access frequency
func (d *db) GetWriters(dbName, key string) (*types.ResponseEnvelope, error) {
	writers, err := d.provenanceQueryProcessor.GetWriters(dbName, key)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(writers)
}

// GetTxIDsSubmittedByUser returns all ids of all transactions submitted by a given user
func (d *db) GetTxIDsSubmittedByUser(userID string) (*types.ResponseEnvelope, error) {
	submittedByUser, err := d.provenanceQueryProcessor.GetTxIDsSubmittedByUser(userID)
	if err != nil {
		return nil, err
	}

	return d.signedResponseEnvelope(submittedByUser)
}

// Close closes and release resources used by db
func (d *db) Close() error {
	if err := d.txProcessor.close(); err != nil {
		return errors.WithMessage(err, "error while closing the transaction processor")
	}

	if err := d.db.Close(); err != nil {
		return errors.WithMessage(err, "error while closing the worldstate database")
	}

	if err := d.provenanceStore.Close(); err != nil {
		return errors.WithMessage(err, "error while closing the provenance store")
	}

	if err := d.blockStore.Close(); err != nil {
		return errors.WithMessage(err, "error while closing the block store")
	}

	if err := d.stateTrieStore.Close(); err != nil {
		return errors.WithMessage(err, "error while closing the block store")
	}

	d.logger.Info("Closed internal DB")
	return nil
}

func (d *db) signedResponseEnvelope(response interface{}) (*types.ResponseEnvelope, error) {
	responseBytes, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}

	payload := &types.Payload{
		Header: &types.ResponseHeader{
			NodeID: d.nodeID,
		},
		Response: responseBytes,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	sig, err := d.signer.Sign(payloadBytes)
	if err != nil {
		return nil, err
	}
	return &types.ResponseEnvelope{
		Payload:   payloadBytes,
		Signature: sig,
	}, nil
}

func prepareConfigTx(conf *config.Configurations) (*types.ConfigTxEnvelope, error) {
	certs, err := readCerts(conf)
	if err != nil {
		return nil, err
	}

	inNodes := false
	var nodes []*types.NodeConfig
	for _, node := range conf.SharedConfig.Nodes {
		nc := &types.NodeConfig{
			ID:      node.NodeID,
			Address: node.Host,
			Port:    node.Port,
		}
		if cert, ok := certs.nodeCertificates[node.NodeID]; ok {
			nc.Certificate = cert
		} else {
			return nil, errors.Errorf("Cannot find certificate for node: %s", node.NodeID)
		}
		nodes = append(nodes, nc)

		if node.NodeID == conf.LocalConfig.Server.Identity.ID {
			inNodes = true
		}
	}
	if !inNodes {
		return nil, errors.Errorf("Cannot find local Server.Identity.ID [%s] in SharedConfig.Nodes: %v", conf.LocalConfig.Server.Identity.ID, conf.SharedConfig.Nodes)
	}

	clusterConfig := &types.ClusterConfig{
		Nodes: nodes,
		Admins: []*types.Admin{
			{
				ID:          conf.SharedConfig.Admin.ID,
				Certificate: certs.adminCert,
			},
		},
		CertAuthConfig: certs.caCerts,
		ConsensusConfig: &types.ConsensusConfig{
			Algorithm: conf.SharedConfig.Consensus.Algorithm,
			Members:   make([]*types.PeerConfig, len(conf.SharedConfig.Consensus.Members)),
			Observers: make([]*types.PeerConfig, len(conf.SharedConfig.Consensus.Observers)),
			RaftConfig: &types.RaftConfig{
				TickInterval:   conf.SharedConfig.Consensus.RaftConfig.TickInterval,
				ElectionTicks:  conf.SharedConfig.Consensus.RaftConfig.ElectionTicks,
				HeartbeatTicks: conf.SharedConfig.Consensus.RaftConfig.HeartbeatTicks,
			},
		},
	}

	inMembers := false
	for i, m := range conf.SharedConfig.Consensus.Members {
		clusterConfig.ConsensusConfig.Members[i] = &types.PeerConfig{
			NodeId:   m.NodeId,
			RaftId:   m.RaftId,
			PeerHost: m.PeerHost,
			PeerPort: m.PeerPort,
		}
		if m.NodeId == conf.LocalConfig.Server.Identity.ID {
			inMembers = true
		}
	}

	inObservers := false
	for i, m := range conf.SharedConfig.Consensus.Observers {
		clusterConfig.ConsensusConfig.Observers[i] = &types.PeerConfig{
			NodeId:   m.NodeId,
			RaftId:   m.RaftId,
			PeerHost: m.PeerHost,
			PeerPort: m.PeerPort,
		}
		if m.NodeId == conf.LocalConfig.Server.Identity.ID {
			inObservers = true
		}
	}

	if !inMembers && !inObservers {
		return nil, errors.Errorf("Cannot find local Server.Identity.ID [%s] in SharedConfig.Consensus Members or Observers: %v",
			conf.LocalConfig.Server.Identity.ID, conf.SharedConfig.Consensus)
	}
	if inObservers && inMembers {
		return nil, errors.Errorf("local Server.Identity.ID [%s] cannot be in SharedConfig.Consensus both Members and Observers: %v",
			conf.LocalConfig.Server.Identity.ID, conf.SharedConfig.Consensus)
	}
	// TODO add support for observers, see issue: https://github.ibm.com/blockchaindb/server/issues/403
	if inObservers {
		return nil, errors.Errorf("not supported yet: local Server.Identity.ID [%s] is in SharedConfig.Consensus.Observers: %v",
			conf.LocalConfig.Server.Identity.ID, conf.SharedConfig.Consensus)
	}

	return &types.ConfigTxEnvelope{
		Payload: &types.ConfigTx{
			TxID:      uuid.New().String(), // TODO: we need to change TxID to string
			NewConfig: clusterConfig,
		},
		// TODO: we can make the node itself sign the transaction
	}, nil
}

type certsInGenesisConfig struct {
	nodeCertificates map[string][]byte
	adminCert        []byte
	caCerts          *types.CAConfig
}

func readCerts(conf *config.Configurations) (*certsInGenesisConfig, error) {
	certsInGen := &certsInGenesisConfig{
		nodeCertificates: make(map[string][]byte),
		caCerts:          &types.CAConfig{},
	}

	for _, node := range conf.SharedConfig.Nodes {
		nodeCert, err := ioutil.ReadFile(node.CertificatePath)
		if err != nil {
			return nil, errors.Wrapf(err, "error while reading node certificate: %s", node.CertificatePath)
		}
		nodePemCert, _ := pem.Decode(nodeCert)
		certsInGen.nodeCertificates[node.NodeID] = nodePemCert.Bytes
	}

	adminCert, err := ioutil.ReadFile(conf.SharedConfig.Admin.CertificatePath)
	if err != nil {
		return nil, errors.Wrapf(err, "error while reading admin certificate %s", conf.SharedConfig.Admin.CertificatePath)
	}
	adminPemCert, _ := pem.Decode(adminCert)
	certsInGen.adminCert = adminPemCert.Bytes

	for _, certPath := range conf.SharedConfig.CAConfig.RootCACertsPath {
		rootCACert, err := ioutil.ReadFile(certPath)
		if err != nil {
			return nil, errors.Wrapf(err, "error while reading root CA certificate %s", certPath)
		}
		caPemCert, _ := pem.Decode(rootCACert)
		certsInGen.caCerts.Roots = append(certsInGen.caCerts.Roots, caPemCert.Bytes)
	}

	for _, certPath := range conf.SharedConfig.CAConfig.IntermediateCACertsPath {
		caCert, err := ioutil.ReadFile(certPath)
		if err != nil {
			return nil, errors.Wrapf(err, "error while reading intermediate CA certificate %s", certPath)
		}
		caPemCert, _ := pem.Decode(caCert)
		certsInGen.caCerts.Intermediates = append(certsInGen.caCerts.Intermediates, caPemCert.Bytes)
	}

	return certsInGen, nil
}

func createLedgerDir(dir string) error {
	exist, err := fileops.Exists(dir)
	if err != nil {
		return err
	}
	if exist {
		return nil
	}

	return fileops.CreateDir(dir)
}
