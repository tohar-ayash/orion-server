// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockprocessor

import (
	"crypto/x509"
	"testing"

	"github.com/IBM-Blockchain/bcdb-server/internal/identity"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/pkg/server/testutils"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestValidateUsedAdminTx(t *testing.T) {
	t.Parallel()

	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"adminUser", "nonAdminUser", "user1"})
	adminCert, adminSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "adminUser")
	nonAdminCert, nonAdminSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "nonAdminUser")
	user1Cert, _ := testutils.LoadTestClientCrypto(t, cryptoDir, "nonAdminUser")
	caCert, _ := testutils.LoadTestClientCA(t, cryptoDir, testutils.RootCAFileName)

	nonAdminUser := &types.User{
		ID:          "nonAdminUser",
		Certificate: nonAdminCert.Raw,
	}
	nonAdminUserSerialized, err := proto.Marshal(nonAdminUser)
	require.NoError(t, err)

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
		name           string
		setup          func(db worldstate.DB)
		txEnv          *types.UserAdministrationTxEnvelope
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: signature verification failure",
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
			txEnv: testutils.SignedUserAdministrationTxEnvelope(t, nonAdminSigner, &types.UserAdministrationTx{
				UserID: "adminUser",
				UserReads: []*types.UserRead{
					{
						UserID: "user1",
					},
					{
						UserID: "user2",
					},
				},
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_UNAUTHORISED,
				ReasonIfInvalid: "signature verification failed: x509: ECDSA verification failure",
			},
		},
		{
			name: "invalid: submitter does not have user admin privilege",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   string(identity.UserNamespace) + "nonAdminUser",
								Value: nonAdminUserSerialized,
							},
						},
					},
				}

				require.NoError(t, db.Commit(newUsers, 1))
			},
			txEnv: testutils.SignedUserAdministrationTxEnvelope(t, nonAdminSigner,
				&types.UserAdministrationTx{
					UserID: "nonAdminUser",
				}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [nonAdminUser] has no privilege to perform user administrative operations",
			},
		},
		{
			name: "invalid: userID in the write list is empty",
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
			txEnv: testutils.SignedUserAdministrationTxEnvelope(t, adminSigner,
				&types.UserAdministrationTx{
					UserID: "adminUser",
					UserWrites: []*types.UserWrite{
						{
							User: &types.User{
								ID: "",
							},
						},
					},
				}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an user in the write list with an empty ID. A valid userID must be an non-empty string",
			},
		},
		{
			name: "invalid: userID in the delete list is empty",
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
			txEnv: testutils.SignedUserAdministrationTxEnvelope(t, adminSigner,
				&types.UserAdministrationTx{
					UserID: "adminUser",
					UserDeletes: []*types.UserDelete{
						{
							UserID: "",
						},
					},
				}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an user in the delete list with an empty ID. A valid userID must be an non-empty string",
			},
		},
		{
			name: "invalid: duplicate userID in the delete list",
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
			txEnv: testutils.SignedUserAdministrationTxEnvelope(t, adminSigner,
				&types.UserAdministrationTx{
					UserID: "adminUser",
					UserDeletes: []*types.UserDelete{
						{
							UserID: "user1",
						},
						{
							UserID: "user1",
						},
					},
				}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two users with the same userID [user1] in the delete list. The userIDs in the delete list must be unique",
			},
		},
		{
			name: "invalid: acl on reads does not pass",
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
									"user2": true,
								},
							}),
						},
					},
				}

				require.NoError(t, db.Commit(newUsers, 1))
			},
			txEnv: testutils.SignedUserAdministrationTxEnvelope(t, adminSigner,
				&types.UserAdministrationTx{
					UserID: "adminUser",
					UserReads: []*types.UserRead{
						{
							UserID: "user1",
						},
					},
				}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [adminUser] has no read permission on the user [user1]",
			},
		},
		{
			name: "invalid: acl on write does not pass",
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
								ReadWriteUsers: map[string]bool{
									"user2": true,
								},
							}),
						},
					},
				}

				require.NoError(t, db.Commit(newUsers, 1))
			},
			txEnv: testutils.SignedUserAdministrationTxEnvelope(t, adminSigner,
				&types.UserAdministrationTx{
					UserID: "adminUser",
					UserWrites: []*types.UserWrite{
						{
							User: &types.User{
								ID:          "user1",
								Certificate: user1Cert.Raw,
							},
						},
					},
				}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [adminUser] has no write permission on the user [user1]",
			},
		},
		{
			name: "invalid: acl on deletes does not pass",
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
								ReadWriteUsers: map[string]bool{
									"user2": true,
								},
							}),
						},
					},
				}

				require.NoError(t, db.Commit(newUsers, 1))
			},
			txEnv: testutils.SignedUserAdministrationTxEnvelope(t, adminSigner,
				&types.UserAdministrationTx{
					UserID: "adminUser",
					UserDeletes: []*types.UserDelete{
						{
							UserID: "user1",
						},
					},
				}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [adminUser] has no write permission on the user [user1]. Hence, the delete operation cannot be performed",
			},
		},
		{
			name: "invalid: mvcc validation does not pass",
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
			txEnv: testutils.SignedUserAdministrationTxEnvelope(t, adminSigner,
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
				}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the user [user1] has changed",
			},
		},
		{
			name: "valid",
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
			txEnv: testutils.SignedUserAdministrationTxEnvelope(t, adminSigner, &types.UserAdministrationTx{
				UserID: "adminUser",
				UserReads: []*types.UserRead{
					{
						UserID: "user1",
					},
					{
						UserID: "user2",
					},
				},
			}),
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
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

			result, err := env.validator.userAdminTxValidator.validate(tt.txEnv)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateEntryFieldsInWrites(t *testing.T) {
	t.Parallel()

	userID := "alice"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	aliceCert, _ := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")
	caCert, _ := testutils.LoadTestClientCA(t, cryptoDir, testutils.RootCAFileName)

	untrustedCryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	untrustedAliceCert, _ := testutils.LoadTestClientCrypto(t, untrustedCryptoDir, "alice")

	tests := []struct {
		name           string
		userWrites     []*types.UserWrite
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: nil entry in the write list",
			userWrites: []*types.UserWrite{
				nil,
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty entry in the write list",
			},
		},
		{
			name: "invalid: nil user entry in the write list",
			userWrites: []*types.UserWrite{
				{},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty user entry in the write list",
			},
		},
		{
			name: "invalid: a userID in the write list is empty",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: "",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an user in the write list with an empty ID. A valid userID must be an non-empty string",
			},
		},
		{
			name: "invalid: the user is marked as admin",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: "user1",
						Privilege: &types.Privilege{
							Admin: true,
						},
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [user1] is marked as admin user. Only via a cluster configuration transaction, the [user1] can be added as admin",
			},
		},
		{
			name: "invalid: db present in the premission list does not exist",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: "user1",
						Privilege: &types.Privilege{
							DBPermission: map[string]types.Privilege_Access{
								"db1": types.Privilege_Read,
							},
						},
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_DATABASE_DOES_NOT_EXIST,
				ReasonIfInvalid: "the database [db1] present in the db permission list does not exist in the cluster",
			},
		},
		{
			name: "invalid: certificate is not valid",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID:          "user1",
						Certificate: []byte("random"),
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the user [user1] in the write list has an invalid certificate: Error = error parsing certificate: asn1: structure error: tags don't match (16 vs {class:1 tag:18 length:97 isCompound:true}) {optional:false explicit:false application:false private:false defaultValue:<nil> tag:<nil> stringType:0 timeType:0 set:false omitEmpty:false} certificate @2",
			},
		},
		{
			name: "invalid: user certificate is not from trusted CA",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID:          userID,
						Certificate: untrustedAliceCert.Raw,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the user [alice] in the write list has an invalid certificate: Error = error verifying certificate against trusted certificate authority (CA): x509: certificate signed by unknown authority (possibly because of \"x509: ECDSA verification failure\" while trying to verify candidate authority certificate \"Clients RootCA\")",
			},
		},
		{
			name: "valid: entries are correct",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID:          userID,
						Certificate: aliceCert.Raw,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid: entries are correct and db exist too",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: userID,
						Privilege: &types.Privilege{
							DBPermission: map[string]types.Privilege_Access{
								"bdb": types.Privilege_Read,
							},
						},
						Certificate: aliceCert.Raw,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:       "valid: no writes",
			userWrites: nil,
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
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

			result, err := env.validator.userAdminTxValidator.validateFieldsInUserWrites(tt.userWrites)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateEntryFieldsInDeletes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		userDeletes    []*types.UserDelete
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: an empty userID in the delete list",
			userDeletes: []*types.UserDelete{
				nil,
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty entry in the delete list",
			},
		},
		{
			name: "invalid: a userID in the delete list is empty",
			userDeletes: []*types.UserDelete{
				{
					UserID: "",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an user in the delete list with an empty ID. A valid userID must be an non-empty string",
			},
		},
		{
			name: "valid: entries are correct",
			userDeletes: []*types.UserDelete{
				{
					UserID: "user2",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:        "valid: no deletes",
			userDeletes: nil,
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := validateFieldsInUserDeletes(tt.userDeletes)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateUniquenessInEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		userWrites     []*types.UserWrite
		userDeletes    []*types.UserDelete
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: a userID is duplicated in the write list",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: "user1",
					},
				},
				{
					User: &types.User{
						ID: "user1",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two users with the same userID [user1] in the write list. The userIDs in the write list must be unique",
			},
		},
		{
			name: "invalid: duplicate userID in the delete list",
			userDeletes: []*types.UserDelete{
				{
					UserID: "user2",
				},
				{
					UserID: "user2",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two users with the same userID [user2] in the delete list. The userIDs in the delete list must be unique",
			},
		},
		{
			name: "invalid: a userID present in both write and delete list",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: "user1",
					},
				},
			},
			userDeletes: []*types.UserDelete{
				{
					UserID: "user1",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the user [user1] is present in both write and delete list. Only one operation per key is allowed within a transaction",
			},
		},
		{
			name: "valid: entries are correct",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: "user1",
					},
				},
			},
			userDeletes: []*types.UserDelete{
				{
					UserID: "user2",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:        "valid: no writes and deletes",
			userWrites:  nil,
			userDeletes: nil,
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := validateUniquenessInUserWritesAndDeletes(tt.userWrites, tt.userDeletes)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateACLOnUserReads(t *testing.T) {
	t.Parallel()

	sampleVersion := &types.Version{
		BlockNum: 2,
		TxNum:    1,
	}

	tests := []struct {
		name           string
		operatingUser  string
		setup          func(db worldstate.DB)
		userReads      []*types.UserRead
		expectedResult *types.ValidationInfo
	}{
		{
			name:          "invalid: operatingUser does not have read permission on user2",
			operatingUser: "operatingUser",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "operatingUser", nil, nil, sampleVersion, nil),
							constructUserForTest(t, "user1", nil, nil, sampleVersion, &types.AccessControl{
								ReadWriteUsers: map[string]bool{
									"operatingUser": true,
								},
							}),
							constructUserForTest(t, "user2", nil, nil, sampleVersion, &types.AccessControl{
								ReadWriteUsers: map[string]bool{
									"user1": true,
								},
							}),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userReads: []*types.UserRead{
				{
					UserID: "user1",
				},
				{
					UserID: "user2",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [operatingUser] has no read permission on the user [user2]",
			},
		},
		{
			name:          "valid: acl check passes",
			operatingUser: "operatingUser",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "operatingUser", nil, nil, sampleVersion, nil),
							constructUserForTest(t, "user1", nil, nil, sampleVersion, &types.AccessControl{
								ReadWriteUsers: map[string]bool{
									"operatingUser": true,
								},
							}),
							constructUserForTest(t, "user2", nil, nil, sampleVersion, &types.AccessControl{
								ReadWriteUsers: map[string]bool{
									"operatingUser": true,
								},
							}),
							constructUserForTest(t, "user3", nil, nil, sampleVersion, nil),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userReads: []*types.UserRead{
				{
					UserID: "user1",
				},
				{
					UserID: "user2",
				},
				{
					UserID: "user3",
				},
				{
					UserID: "user4",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:          "valid: empty reads",
			operatingUser: "operatingUser",
			setup:         func(db worldstate.DB) {},
			userReads:     nil,
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
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

			result, err := env.validator.userAdminTxValidator.validateACLOnUserReads(tt.operatingUser, tt.userReads)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateACLOnUserWrites(t *testing.T) {
	t.Parallel()

	sampleVersion := &types.Version{
		BlockNum: 2,
		TxNum:    1,
	}
	adminEntry := constructUserForTest(t, "admin", nil, nil, sampleVersion, nil)
	admUsr := &types.User{}
	require.NoError(t, proto.Unmarshal(adminEntry.Value, admUsr))
	admUsr.Privilege = &types.Privilege{
		Admin: true,
	}
	userSerialized, err := proto.Marshal(admUsr)
	require.NoError(t, err)
	adminEntry.Value = userSerialized

	tests := []struct {
		name           string
		operatingUser  string
		setup          func(db worldstate.DB)
		userWrites     []*types.UserWrite
		expectedResult *types.ValidationInfo
	}{
		{
			name:          "invalid: targetUser is an admin",
			operatingUser: "operatingUser",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "operatingUser", nil, nil, sampleVersion, nil),
							adminEntry,
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: "admin",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [admin] is an admin user. Only via a cluster configuration transaction, the [admin] can be modified",
			},
		},
		{
			name:          "invalid: operatingUser does not have write permission on user2",
			operatingUser: "operatingUser",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "operatingUser", nil, nil, sampleVersion, nil),
							constructUserForTest(t, "user1", nil, nil, sampleVersion, &types.AccessControl{
								ReadWriteUsers: map[string]bool{
									"operatingUser": true,
								},
							}),
							constructUserForTest(t, "user2", nil, nil, sampleVersion, &types.AccessControl{
								ReadWriteUsers: map[string]bool{
									"user1": true,
								},
							}),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: "user1",
					},
				},
				{
					User: &types.User{
						ID: "user2",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [operatingUser] has no write permission on the user [user2]",
			},
		},
		{
			name:          "valid: acl check passes",
			operatingUser: "operatingUser",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "operatingUser", nil, nil, sampleVersion, nil),
							constructUserForTest(t, "user1", nil, nil, sampleVersion, &types.AccessControl{
								ReadWriteUsers: map[string]bool{
									"operatingUser": true,
								},
							}),
							constructUserForTest(t, "user2", nil, nil, sampleVersion, &types.AccessControl{
								ReadWriteUsers: map[string]bool{
									"operatingUser": true,
								},
							}),
							constructUserForTest(t, "user3", nil, nil, sampleVersion, nil),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: "user1",
					},
				},
				{
					User: &types.User{
						ID: "user2",
					},
				},
				{
					User: &types.User{
						ID: "user3",
					},
				},
				{
					User: &types.User{
						ID: "user4",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:          "valid: empty writes",
			operatingUser: "operatingUser",
			setup:         func(db worldstate.DB) {},
			userWrites:    nil,
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
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

			result, err := env.validator.userAdminTxValidator.validateACLOnUserWrites(tt.operatingUser, tt.userWrites)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateACLOnUserDeletes(t *testing.T) {
	t.Parallel()

	sampleVersion := &types.Version{
		BlockNum: 2,
		TxNum:    1,
	}
	adminEntry := constructUserForTest(t, "admin", nil, nil, sampleVersion, nil)
	admUsr := &types.User{}
	require.NoError(t, proto.Unmarshal(adminEntry.Value, admUsr))
	admUsr.Privilege = &types.Privilege{
		Admin: true,
	}
	userSerialized, err := proto.Marshal(admUsr)
	require.NoError(t, err)
	adminEntry.Value = userSerialized

	tests := []struct {
		name           string
		operatingUser  string
		setup          func(db worldstate.DB)
		userDeletes    []*types.UserDelete
		expectedResult *types.ValidationInfo
	}{
		{
			name:          "invalid: targetUser is an admin",
			operatingUser: "operatingUser",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "operatingUser", nil, nil, sampleVersion, nil),
							adminEntry,
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userDeletes: []*types.UserDelete{
				{
					UserID: "admin",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [admin] is an admin user. Only via a cluster configuration transaction, the [admin] can be deleted",
			},
		},
		{
			name:          "invalid: operatingUser does not have write permission on user2",
			operatingUser: "operatingUser",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "operatingUser", nil, nil, sampleVersion, nil),
							constructUserForTest(t, "user1", nil, nil, sampleVersion, &types.AccessControl{
								ReadWriteUsers: map[string]bool{
									"operatingUser": true,
								},
							}),
							constructUserForTest(t, "user2", nil, nil, sampleVersion, &types.AccessControl{
								ReadWriteUsers: map[string]bool{
									"user1": true,
								},
							}),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userDeletes: []*types.UserDelete{
				{
					UserID: "user1",
				},
				{
					UserID: "user2",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [operatingUser] has no write permission on the user [user2]. Hence, the delete operation cannot be performed",
			},
		},
		{
			name:          "invalid: user2 present in the delete list does not exist",
			operatingUser: "operatingUser",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "operatingUser", nil, nil, sampleVersion, nil),
							constructUserForTest(t, "user1", nil, nil, sampleVersion, &types.AccessControl{
								ReadWriteUsers: map[string]bool{
									"operatingUser": true,
								},
							}),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userDeletes: []*types.UserDelete{
				{
					UserID: "user1",
				},
				{
					UserID: "user2",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the user [user2] present in the delete list does not exist",
			},
		},
		{
			name:          "valid: acl check passes",
			operatingUser: "operatingUser",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "operatingUser", nil, nil, sampleVersion, nil),
							constructUserForTest(t, "user1", nil, nil, sampleVersion, &types.AccessControl{
								ReadWriteUsers: map[string]bool{
									"operatingUser": true,
								},
							}),
							constructUserForTest(t, "user2", nil, nil, sampleVersion, &types.AccessControl{
								ReadWriteUsers: map[string]bool{
									"operatingUser": true,
								},
							}),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userDeletes: []*types.UserDelete{
				{
					UserID: "user1",
				},
				{
					UserID: "user2",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:          "valid: empty deletes",
			operatingUser: "operatingUser",
			setup:         func(db worldstate.DB) {},
			userDeletes:   nil,
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
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

			result, err := env.validator.userAdminTxValidator.validateACLOnUserDeletes(tt.operatingUser, tt.userDeletes)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestMVCCOnUserAdminTx(t *testing.T) {
	t.Parallel()

	version1 := &types.Version{
		BlockNum: 1,
		TxNum:    0,
	}

	version2 := &types.Version{
		BlockNum: 2,
		TxNum:    0,
	}

	version3 := &types.Version{
		BlockNum: 3,
		TxNum:    0,
	}

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		userReads      []*types.UserRead
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: no committed state for user2",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "user1", nil, nil, version1, nil),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userReads: []*types.UserRead{
				{
					UserID:  "user1",
					Version: version1,
				},
				{
					UserID:  "user2",
					Version: version1,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the user [user2] has changed",
			},
		},
		{
			name: "invalid: committed state does not match",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "user1", nil, nil, version1, nil),
							constructUserForTest(t, "user2", nil, nil, version3, nil),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userReads: []*types.UserRead{
				{
					UserID:  "user1",
					Version: version1,
				},
				{
					UserID:  "user2",
					Version: version2,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the user [user2] has changed",
			},
		},
		{
			name: "valid: reads matches committed version",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "user1", nil, nil, version1, nil),
							constructUserForTest(t, "user2", nil, nil, version3, nil),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userReads: []*types.UserRead{
				{
					UserID:  "user1",
					Version: version1,
				},
				{
					UserID:  "user2",
					Version: version3,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:  "valid: user does not exist and would match the nil version",
			setup: func(db worldstate.DB) {},
			userReads: []*types.UserRead{
				{
					UserID: "user1",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:      "valid: reads is nil",
			setup:     func(db worldstate.DB) {},
			userReads: nil,
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
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

			result, err := env.validator.userAdminTxValidator.mvccValidation(tt.userReads)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func setupClusterConfigCA(t *testing.T, env *validatorTestEnv, rootCACert *x509.Certificate) {
	config := &types.ClusterConfig{
		CertAuthConfig: &types.CAConfig{
			Roots: [][]byte{rootCACert.Raw},
		},
	}
	configSerialized, err := proto.Marshal(config)
	require.NoError(t, err)
	newConfig := []*worldstate.DBUpdates{
		{
			DBName: worldstate.ConfigDBName,
			Writes: []*worldstate.KVWithMetadata{
				{
					Key:   worldstate.ConfigKey,
					Value: configSerialized,
				},
			},
		},
	}
	require.NoError(t, env.db.Commit(newConfig, 1))
	configR, _, err := env.db.GetConfig()
	require.NoError(t, err)
	require.NotNil(t, configR)
}

func constructUserForTest(t *testing.T, userID string, certRaw []byte, priv *types.Privilege, version *types.Version, acl *types.AccessControl) *worldstate.KVWithMetadata {
	user := &types.User{
		ID:          userID,
		Certificate: certRaw,
		Privilege:   priv,
	}
	userSerialized, err := proto.Marshal(user)
	require.NoError(t, err)

	userEntry := &worldstate.KVWithMetadata{
		Key:   string(identity.UserNamespace) + userID,
		Value: userSerialized,
		Metadata: &types.Metadata{
			Version:       version,
			AccessControl: acl,
		},
	}

	return userEntry
}
