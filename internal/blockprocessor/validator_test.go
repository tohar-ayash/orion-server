// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockprocessor

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/IBM-Blockchain/bcdb-server/internal/identity"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate/leveldb"
	"github.com/IBM-Blockchain/bcdb-server/pkg/crypto"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/server/testutils"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

type validatorTestEnv struct {
	db        *leveldb.LevelDB
	path      string
	validator *validator
	cleanup   func()
}

func newValidatorTestEnv(t *testing.T) *validatorTestEnv {
	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("/tmp", "validator")
	require.NoError(t, err)
	path := filepath.Join(dir, "leveldb")

	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: path,
			Logger:    logger,
		},
	)
	if err != nil {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("failed to remove directory %s, %v", dir, err)
		}
		t.Fatalf("failed to create leveldb with path %s", path)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close the db instance, %v", err)
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("failed to remove directory %s, %v", dir, err)
		}
	}

	return &validatorTestEnv{
		db:   db,
		path: path,
		validator: newValidator(
			&Config{
				DB:     db,
				Logger: logger,
			},
		),
		cleanup: cleanup,
	}
}

func TestValidateGenesisBlock(t *testing.T) {
	t.Parallel()

	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"admin1", "node1"})
	adminCert, _ := testutils.LoadTestClientCrypto(t, cryptoDir, "admin1")
	nodeCert, _ := testutils.LoadTestClientCrypto(t, cryptoDir, "node1")
	caCert, _ := testutils.LoadTestClientCA(t, cryptoDir, testutils.RootCAFileName)

	//TODO test when admin and node cert are not signed by correct CA

	t.Run("genesis block with invalid config tx", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name          string
			genesisBlock  *types.Block
			expectedError string
		}{
			{
				name: "root CA config is missing",
				genesisBlock: &types.Block{
					Header: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number: 1,
						},
					},
					Payload: &types.Block_ConfigTxEnvelope{
						ConfigTxEnvelope: &types.ConfigTxEnvelope{
							Payload: &types.ConfigTx{
								NewConfig: &types.ClusterConfig{
									Nodes: []*types.NodeConfig{
										{
											ID:          "node1",
											Address:     "127.0.0.1",
											Port:        6090,
											Certificate: nodeCert.Raw,
										},
									},
									Admins: []*types.Admin{
										{
											ID:          "admin1",
											Certificate: adminCert.Raw,
										},
									},
								},
							},
						},
					},
				},
				expectedError: "genesis block cannot be invalid: reason for invalidation [CA config is empty. At least one root CA is required]",
			},
			{
				name: "root CA cert is invalid",
				genesisBlock: &types.Block{
					Header: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number: 1,
						},
					},
					Payload: &types.Block_ConfigTxEnvelope{
						ConfigTxEnvelope: &types.ConfigTxEnvelope{
							Payload: &types.ConfigTx{
								NewConfig: &types.ClusterConfig{
									Nodes: []*types.NodeConfig{
										{
											ID:          "node1",
											Address:     "127.0.0.1",
											Port:        6090,
											Certificate: nodeCert.Raw,
										},
									},
									Admins: []*types.Admin{
										{
											ID:          "admin1",
											Certificate: adminCert.Raw,
										},
									},
									CertAuthConfig: &types.CAConfig{
										Roots: [][]byte{[]byte("bad-certificate")},
									},
								},
							},
						},
					},
				},
				expectedError: "genesis block cannot be invalid: reason for invalidation [CA certificate collection cannot be created: asn1: structure error: tags don't match (16 vs {class:1 tag:2 length:97 isCompound:true}) {optional:false explicit:false application:false private:false defaultValue:<nil> tag:<nil> stringType:0 timeType:0 set:false omitEmpty:false} certificate @2]",
			},
			{
				name: "root CA cert is not from a CA",
				genesisBlock: &types.Block{
					Header: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number: 1,
						},
					},
					Payload: &types.Block_ConfigTxEnvelope{
						ConfigTxEnvelope: &types.ConfigTxEnvelope{
							Payload: &types.ConfigTx{
								NewConfig: &types.ClusterConfig{
									Nodes: []*types.NodeConfig{
										{
											ID:          "node1",
											Address:     "127.0.0.1",
											Port:        6090,
											Certificate: nodeCert.Raw,
										},
									},
									Admins: []*types.Admin{
										{
											ID:          "admin1",
											Certificate: adminCert.Raw,
										},
									},
									CertAuthConfig: &types.CAConfig{
										Roots: [][]byte{adminCert.Raw},
									},
								},
							},
						},
					},
				},
				expectedError: "genesis block cannot be invalid: reason for invalidation [CA certificate collection cannot be created: certificate is missing the CA property, SN:",
			},
			{
				name: "node certificate is invalid",
				genesisBlock: &types.Block{
					Header: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number: 1,
						},
					},
					Payload: &types.Block_ConfigTxEnvelope{
						ConfigTxEnvelope: &types.ConfigTxEnvelope{
							Payload: &types.ConfigTx{
								NewConfig: &types.ClusterConfig{
									Nodes: []*types.NodeConfig{
										{
											ID:          "node1",
											Address:     "127.0.0.1",
											Port:        6090,
											Certificate: []byte("random"),
										},
									},
									Admins: []*types.Admin{
										{
											ID:          "admin1",
											Certificate: adminCert.Raw,
										},
									},
									CertAuthConfig: &types.CAConfig{
										Roots: [][]byte{caCert.Raw},
									},
								},
							},
						},
					},
				},
				expectedError: "genesis block cannot be invalid: reason for invalidation [the node [node1] has an invalid certificate",
			},
			{
				name: "admin certificate is invalid",
				genesisBlock: &types.Block{
					Header: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number: 1,
						},
					},
					Payload: &types.Block_ConfigTxEnvelope{
						ConfigTxEnvelope: &types.ConfigTxEnvelope{
							Payload: &types.ConfigTx{
								NewConfig: &types.ClusterConfig{
									Nodes: []*types.NodeConfig{
										{
											ID:          "node1",
											Address:     "127.0.0.1",
											Port:        6090,
											Certificate: nodeCert.Raw,
										},
									},
									Admins: []*types.Admin{
										{
											ID:          "admin1",
											Certificate: []byte("random"),
										},
									},
									CertAuthConfig: &types.CAConfig{
										Roots: [][]byte{caCert.Raw},
									},
								},
							},
						},
					},
				},
				expectedError: "genesis block cannot be invalid: reason for invalidation [the admin [admin1] has an invalid certificate",
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				env := newValidatorTestEnv(t)
				defer env.cleanup()

				results, err := env.validator.validateBlock(tt.genesisBlock)
				require.Contains(t, err.Error(), tt.expectedError)
				require.Nil(t, results)
			})
		}
	})

	t.Run("genesis block with valid config tx", func(t *testing.T) {
		t.Parallel()

		genesisBlock := &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number: 1,
				},
			},
			Payload: &types.Block_ConfigTxEnvelope{
				ConfigTxEnvelope: &types.ConfigTxEnvelope{
					Payload: &types.ConfigTx{
						NewConfig: &types.ClusterConfig{
							Nodes: []*types.NodeConfig{
								{
									ID:          "node1",
									Address:     "127.0.0.1",
									Port:        6090,
									Certificate: nodeCert.Raw,
								},
							},
							Admins: []*types.Admin{
								{
									ID:          "admin1",
									Certificate: adminCert.Raw,
								},
							},
							CertAuthConfig: &types.CAConfig{
								Roots: [][]byte{caCert.Raw},
							},
							ConsensusConfig: &types.ConsensusConfig{
								Algorithm: "raft",
								Members: []*types.PeerConfig{
									{
										NodeId:   "node1",
										RaftId:   1,
										PeerHost: "10.10.10.10",
										PeerPort: 7090,
									},
								},
								RaftConfig: &types.RaftConfig{
									TickInterval:   "100ms",
									ElectionTicks:  100,
									HeartbeatTicks: 10,
								},
							},
						},
					},
				},
			},
		}
		expectedResults := []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
		}

		env := newValidatorTestEnv(t)
		defer env.cleanup()

		results, err := env.validator.validateBlock(genesisBlock)
		require.NoError(t, err)
		require.Equal(t, expectedResults, results)
	})
}

func TestValidateDataBlock(t *testing.T) {
	t.Parallel()

	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"operatingUser"})
	userCert, userSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "operatingUser")

	addUserWithCorrectPrivilege := func(db worldstate.DB) {
		user := &types.User{
			ID:          "operatingUser",
			Certificate: userCert.Raw,
			Privilege: &types.Privilege{
				DBPermission: map[string]types.Privilege_Access{
					worldstate.DefaultDBName: types.Privilege_ReadWrite,
					"db1":                    types.Privilege_ReadWrite,
				},
			},
		}
		userSerialized, err := proto.Marshal(user)
		require.NoError(t, err)

		userAdd := []*worldstate.DBUpdates{
			{
				DBName: worldstate.UsersDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(identity.UserNamespace) + "operatingUser",
						Value: userSerialized,
					},
				},
			},
		}

		require.NoError(t, db.Commit(userAdd, 1))
	}

	tests := []struct {
		name            string
		setup           func(db worldstate.DB)
		block           *types.Block
		expectedResults []*types.ValidationInfo
	}{
		{
			name: "data block with valid and invalid transactions",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
				db1 := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DatabasesDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "db1",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
								},
							},
						},
					},
				}
				require.NoError(t, db.Commit(db1, 1))

				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"operatingUser": true,
										},
									},
								},
							},
							{
								Key: "key2",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"operatingUser": true,
										},
									},
								},
							},
						},
					},
					{
						DBName: "db1",
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
								},
							},
							{
								Key: "key2",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
				},
				Payload: &types.Block_DataTxEnvelopes{
					DataTxEnvelopes: &types.DataTxEnvelopes{
						Envelopes: []*types.DataTxEnvelope{
							testutils.SignedDataTxEnvelope(t, []crypto.Signer{userSigner}, &types.DataTx{
								MustSignUserIDs: []string{"operatingUser"},
								DBOperations: []*types.DBOperation{
									{
										DBName: worldstate.DefaultDBName,
										DataReads: []*types.DataRead{
											{
												Key: "key1",
												Version: &types.Version{
													BlockNum: 1,
													TxNum:    1,
												},
											},
										},
										DataWrites: []*types.DataWrite{
											{
												Key:   "key1",
												Value: []byte("new-val"),
											},
										},
										DataDeletes: []*types.DataDelete{
											{
												Key: "key2",
											},
										},
									},
									{
										DBName: "db1",
										DataReads: []*types.DataRead{
											{
												Key: "key1",
												Version: &types.Version{
													BlockNum: 1,
													TxNum:    1,
												},
											},
										},
										DataWrites: []*types.DataWrite{
											{
												Key:   "key1",
												Value: []byte("new-val"),
											},
										},
										DataDeletes: []*types.DataDelete{
											{
												Key: "key2",
											},
										},
									},
								},
							}),
							testutils.SignedDataTxEnvelope(t, []crypto.Signer{userSigner}, &types.DataTx{
								MustSignUserIDs: []string{"operatingUser"},
								DBOperations: []*types.DBOperation{
									{
										DBName: worldstate.DefaultDBName,
										DataReads: []*types.DataRead{
											{
												Key: "key1",
												Version: &types.Version{
													BlockNum: 1,
													TxNum:    1,
												},
											},
										},
									},
								},
							}),
							testutils.SignedDataTxEnvelope(t, []crypto.Signer{userSigner}, &types.DataTx{
								MustSignUserIDs: []string{"operatingUser"},
								DBOperations: []*types.DBOperation{
									{
										DBName: worldstate.DefaultDBName,
										DataReads: []*types.DataRead{
											{
												Key: "key2",
												Version: &types.Version{
													BlockNum: 1,
													TxNum:    1,
												},
											},
										},
									},
								},
							}),
							testutils.SignedDataTxEnvelope(t, []crypto.Signer{userSigner}, &types.DataTx{
								MustSignUserIDs: []string{"operatingUser"},
								DBOperations: []*types.DBOperation{
									{
										DBName: worldstate.DefaultDBName,
									},
									{
										DBName: "db2",
									},
								},
							}),
						},
					},
				},
			},
			expectedResults: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
				{
					Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
					ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [key1] in database [" + worldstate.DefaultDBName + "]",
				},
				{
					Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
					ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [key2] in database [" + worldstate.DefaultDBName + "]",
				},
				{
					Flag:            types.Flag_INVALID_DATABASE_DOES_NOT_EXIST,
					ReasonIfInvalid: "the database [db2] does not exist in the cluster",
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()

			tt.setup(env.db)

			results, err := env.validator.validateBlock(tt.block)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResults, results)
		})
	}
}

func TestValidateUserBlock(t *testing.T) {
	t.Parallel()

	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"adminUser"})
	adminCert, adminSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "adminUser")
	caCert, _ := testutils.LoadTestClientCA(t, cryptoDir, testutils.RootCAFileName)
	require.True(t, caCert.IsCA)

	adminUser := &types.User{
		ID:          "adminUser",
		Certificate: adminCert.Raw,
		Privilege: &types.Privilege{
			Admin: true,
		},
	}
	adminUserSerialized, err := proto.Marshal(adminUser)
	require.NoError(t, err)

	sampleVersion := &types.Version{
		BlockNum: 2,
		TxNum:    1,
	}

	tests := []struct {
		name            string
		setup           func(db worldstate.DB)
		block           *types.Block
		expectedResults []*types.ValidationInfo
	}{
		{
			name: "user block with an invalid transaction",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   string(identity.UserNamespace) + "adminUser",
								Value: adminUserSerialized,
							},
							constructUserForTest(t, "user1", nil, nil, sampleVersion, &types.AccessControl{
								ReadUsers: map[string]bool{
									"adminUser": true,
								},
							}),
						},
					},
				}

				require.NoError(t, db.Commit(newUsers, 1))
			},
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
				},
				Payload: &types.Block_UserAdministrationTxEnvelope{
					UserAdministrationTxEnvelope: testutils.SignedUserAdministrationTxEnvelope(t, adminSigner,
						&types.UserAdministrationTx{
							UserID: "adminUser",
							UserReads: []*types.UserRead{
								{
									UserID: "user1",
									Version: &types.Version{
										BlockNum: 100,
										TxNum:    1000,
									},
								},
							},
						},
					),
				},
			},
			expectedResults: []*types.ValidationInfo{
				{
					Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
					ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the user [user1] has changed",
				},
			},
		},
		{
			name: "user block with an valid transaction",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   string(identity.UserNamespace) + "adminUser",
								Value: adminUserSerialized,
							},
						},
					},
				}

				require.NoError(t, db.Commit(newUsers, 1))
			},
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
				},
				Payload: &types.Block_UserAdministrationTxEnvelope{
					UserAdministrationTxEnvelope: testutils.SignedUserAdministrationTxEnvelope(t, adminSigner,
						&types.UserAdministrationTx{
							UserID: "adminUser",
							UserReads: []*types.UserRead{
								{
									UserID: "user1",
								},
								{
									UserID: "user2",
								},
							},
						},
					),
				},
			},
			expectedResults: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()
			setupClusterConfigCA(t, env, caCert)
			tt.setup(env.db)

			results, err := env.validator.validateBlock(tt.block)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResults, results)
		})
	}
}

func TestValidateDBBlock(t *testing.T) {
	t.Parallel()

	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"userWithMorePrivilege", "userWithLessPrivilege"})
	adminCert, adminSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "userWithMorePrivilege")

	setup := func(db worldstate.DB) {
		userWithMorePrivilege := &types.User{
			ID:          "userWithMorePrivilege",
			Certificate: adminCert.Raw,
			Privilege: &types.Privilege{
				Admin: true,
			},
		}
		userWithMorePrivilegeSerialized, err := proto.Marshal(userWithMorePrivilege)
		require.NoError(t, err)

		privilegedUser := []*worldstate.DBUpdates{
			{
				DBName: worldstate.UsersDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(identity.UserNamespace) + "userWithMorePrivilege",
						Value: userWithMorePrivilegeSerialized,
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    1,
							},
						},
					},
				},
			},
		}
		require.NoError(t, db.Commit(privilegedUser, 1))

		dbs := []*worldstate.DBUpdates{
			{
				DBName: worldstate.DatabasesDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key: "db3",
					},
					{
						Key: "db4",
					},
				},
			},
		}
		require.NoError(t, db.Commit(dbs, 1))
	}

	tests := []struct {
		name            string
		block           *types.Block
		expectedResults []*types.ValidationInfo
	}{
		{
			name: "db block with an invalid transaction",
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
				},
				Payload: &types.Block_DBAdministrationTxEnvelope{
					DBAdministrationTxEnvelope: testutils.SignedDBAdministrationTxEnvelope(t, adminSigner,
						&types.DBAdministrationTx{
							UserID:    "userWithMorePrivilege",
							DeleteDBs: []string{"db1"},
						}),
				},
			},
			expectedResults: []*types.ValidationInfo{
				{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the database [db1] does not exist in the cluster and hence, it cannot be deleted",
				},
			},
		},
		{
			name: "db block with a valid transaction",
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
				},
				Payload: &types.Block_DBAdministrationTxEnvelope{
					DBAdministrationTxEnvelope: testutils.SignedDBAdministrationTxEnvelope(t, adminSigner,
						&types.DBAdministrationTx{
							UserID:    "userWithMorePrivilege",
							CreateDBs: []string{"db1", "db2"},
							DeleteDBs: []string{"db3", "db4"},
						}),
				},
			},
			expectedResults: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()

			setup(env.db)

			results, err := env.validator.validateBlock(tt.block)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResults, results)
		})
	}
}

func TestValidateConfigBlock(t *testing.T) {
	t.Parallel()

	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"adminUser", "admin1", "node1"})
	userCert, userSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "adminUser")
	adminCert, _ := testutils.LoadTestClientCrypto(t, cryptoDir, "admin1")
	nodeCert, _ := testutils.LoadTestClientCrypto(t, cryptoDir, "node1")
	caCert, _ := testutils.LoadTestClientCA(t, cryptoDir, testutils.RootCAFileName)

	setup := func(db worldstate.DB) {
		adminUser := &types.User{
			ID:          "adminUser",
			Certificate: userCert.Raw,
			Privilege: &types.Privilege{
				Admin: true,
			},
		}
		adminUserSerialized, err := proto.Marshal(adminUser)
		require.NoError(t, err)

		newUsers := []*worldstate.DBUpdates{
			{
				DBName: worldstate.UsersDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(identity.UserNamespace) + "adminUser",
						Value: adminUserSerialized,
					},
				},
			},
		}

		require.NoError(t, db.Commit(newUsers, 1))
	}

	tests := []struct {
		name            string
		block           *types.Block
		expectedResults []*types.ValidationInfo
	}{
		{
			name: "config block with an invalid transaction",
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
				},
				Payload: &types.Block_ConfigTxEnvelope{
					ConfigTxEnvelope: testutils.SignedConfigTxEnvelope(t, userSigner,
						&types.ConfigTx{
							UserID: "adminUser",
							ReadOldConfigVersion: &types.Version{
								BlockNum: 100,
								TxNum:    100,
							},
							NewConfig: &types.ClusterConfig{
								Nodes: []*types.NodeConfig{
									{
										ID:          "node1",
										Address:     "127.0.0.1",
										Port:        6090,
										Certificate: nodeCert.Raw,
									},
								},
								Admins: []*types.Admin{
									{
										ID:          "admin1",
										Certificate: adminCert.Raw,
									},
								},
								CertAuthConfig: &types.CAConfig{
									Roots: [][]byte{caCert.Raw},
								},
								ConsensusConfig: &types.ConsensusConfig{
									Algorithm: "raft",
									Members: []*types.PeerConfig{
										{
											NodeId:   "node1",
											RaftId:   1,
											PeerHost: "10.10.10.10",
											PeerPort: 7090,
										},
									},
									RaftConfig: &types.RaftConfig{
										TickInterval:   "100ms",
										ElectionTicks:  100,
										HeartbeatTicks: 10,
									},
								},
							},
						}),
				},
			},
			expectedResults: []*types.ValidationInfo{
				{
					Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
					ReasonIfInvalid: "mvcc conflict has occurred as the read old configuration does not match the committed version",
				},
			},
		},
		{
			name: "config block with a valid transaction",
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
				},
				Payload: &types.Block_ConfigTxEnvelope{
					ConfigTxEnvelope: testutils.SignedConfigTxEnvelope(t, userSigner,
						&types.ConfigTx{
							UserID:               "adminUser",
							ReadOldConfigVersion: nil,
							NewConfig: &types.ClusterConfig{
								Nodes: []*types.NodeConfig{
									{
										ID:          "node1",
										Address:     "127.0.0.1",
										Port:        6090,
										Certificate: nodeCert.Raw,
									},
								},
								Admins: []*types.Admin{
									{
										ID:          "admin1",
										Certificate: adminCert.Raw,
									},
								},
								CertAuthConfig: &types.CAConfig{
									Roots: [][]byte{caCert.Raw},
								},
								ConsensusConfig: &types.ConsensusConfig{
									Algorithm: "raft",
									Members: []*types.PeerConfig{
										{
											NodeId:   "node1",
											RaftId:   1,
											PeerHost: "10.10.10.10",
											PeerPort: 7090,
										},
									},
									RaftConfig: &types.RaftConfig{
										TickInterval:   "100ms",
										ElectionTicks:  100,
										HeartbeatTicks: 10,
									},
								},
							},
						}),
				},
			},
			expectedResults: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()

			setup(env.db)

			results, err := env.validator.validateBlock(tt.block)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResults, results)
		})
	}
}
