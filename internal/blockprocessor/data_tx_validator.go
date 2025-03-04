// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockprocessor

import (
	"sort"
	"strings"

	"github.com/IBM-Blockchain/bcdb-server/internal/identity"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

type dataTxValidator struct {
	db              worldstate.DB
	identityQuerier *identity.Querier
	sigValidator    *txSigValidator
	logger          *logger.SugarLogger
}

func (v *dataTxValidator) validate(txEnv *types.DataTxEnvelope, pendingOps *pendingOperations) (*types.ValidationInfo, error) {
	var valRes *types.ValidationInfo
	var err error

	var userIDsWithValidSign []string
	for userID, signtature := range txEnv.Signatures {
		valRes, err = v.sigValidator.validate(userID, signtature, txEnv.Payload)
		if err != nil {
			return nil, err
		}
		if valRes.Flag != types.Flag_VALID {
			for _, mustSignUserID := range txEnv.Payload.MustSignUserIDs {
				if userID == mustSignUserID {
					return &types.ValidationInfo{
						Flag:            types.Flag_INVALID_UNAUTHORISED,
						ReasonIfInvalid: "signature of the must sign user [" + userID + "] is not valid (maybe the certifcate got changed)",
					}, nil
				}
			}
			continue
		}

		userIDsWithValidSign = append(userIDsWithValidSign, userID)
	}

	dbs := make(map[string]bool)
	for _, ops := range txEnv.Payload.DBOperations {
		if !dbs[ops.DBName] {
			dbs[ops.DBName] = true
			continue
		}

		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "the database [" + ops.DBName + "] occurs more than once in the operations. The database present in the operations should be unique",
		}, nil
	}

	for _, ops := range txEnv.Payload.DBOperations {
		valRes, err = v.validateDBName(ops.DBName)
		if err != nil {
			return nil, err
		}
		if valRes.Flag != types.Flag_VALID {
			return valRes, nil
		}

		var usersWithDBAccess []string
		sort.Strings(userIDsWithValidSign)

		for _, userID := range userIDsWithValidSign {
			// note that the transaction could have been signed by many users and a data tx can manipulate
			// multiple databases. Not all users in the transaction might have read-write access on all databases
			// manipulated by the transaction. Hence, while validating operations associated with a given database,
			// we need to consider only users who have read-write access to it. If none of the user has a
			// read-write permission on a given database, the transaction would be marked invalid.
			hasPerm, err := v.identityQuerier.HasReadWriteAccess(userID, ops.DBName)
			if err != nil {
				return nil, err
			}
			if hasPerm {
				usersWithDBAccess = append(usersWithDBAccess, userID)
			}
		}

		if len(usersWithDBAccess) == 0 {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "none of the user in [" + strings.Join(userIDsWithValidSign, ", ") + "] has read-write permission on the database [" + ops.DBName + "]",
			}, nil
		}

		valRes, err = v.validateOps(usersWithDBAccess, ops, pendingOps)
		if err != nil || valRes.Flag != types.Flag_VALID {
			return valRes, err
		}
	}

	return valRes, nil
}

func (v *dataTxValidator) validateDBName(dbName string) (*types.ValidationInfo, error) {
	switch {
	case !v.db.ValidDBName(dbName):
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "the database name [" + dbName + "] is not valid",
		}, nil

	case !v.db.Exist(dbName):
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_DATABASE_DOES_NOT_EXIST,
			ReasonIfInvalid: "the database [" + dbName + "] does not exist in the cluster",
		}, nil

	case worldstate.IsSystemDB(dbName):
		return &types.ValidationInfo{
			Flag: types.Flag_INVALID_NO_PERMISSION,
			ReasonIfInvalid: "the database [" + dbName + "] is a system database and no user can write to a " +
				"system database via data transaction. Use appropriate transaction type to modify the system database",
		}, nil
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *dataTxValidator) validateOps(
	userIDs []string,
	txOps *types.DBOperation,
	pendingOps *pendingOperations,
) (*types.ValidationInfo, error) {
	dbName := txOps.DBName

	r, err := v.validateFieldsInDataWrites(txOps.DataWrites)
	if err != nil {
		return nil, err
	}
	if r.Flag != types.Flag_VALID {
		return r, nil
	}

	r, err = v.validateFieldsInDataDeletes(txOps.DBName, txOps.DataDeletes, pendingOps)
	if err != nil {
		return nil, err
	}
	if r.Flag != types.Flag_VALID {
		return r, nil
	}

	r = validateUniquenessInDataWritesAndDeletes(txOps.DataWrites, txOps.DataDeletes)
	if r.Flag != types.Flag_VALID {
		return r, nil
	}

	r, err = v.validateACLOnDataReads(userIDs, dbName, txOps.DataReads)
	if err != nil {
		return nil, err
	}
	if r.Flag != types.Flag_VALID {
		return r, nil
	}

	r, err = v.validateACLOnDataWrites(userIDs, dbName, txOps.DataWrites)
	if err != nil {
		return nil, err
	}
	if r.Flag != types.Flag_VALID {
		return r, nil
	}

	r, err = v.validateACLOnDataDeletes(userIDs, dbName, txOps.DataDeletes)
	if err != nil {
		return nil, err
	}
	if r.Flag != types.Flag_VALID {
		return r, nil
	}

	return v.mvccValidation(dbName, txOps.DataReads, pendingOps)
}

func (v *dataTxValidator) validateFieldsInDataWrites(DataWrites []*types.DataWrite) (*types.ValidationInfo, error) {
	existingUser := make(map[string]bool)

	for _, w := range DataWrites {
		if w == nil {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty entry in the write list",
			}, nil
		}

		if w.ACL == nil {
			continue
		}

		userToCheck := make(map[string]struct{})

		for user := range w.ACL.ReadUsers {
			if existingUser[user] {
				continue
			}
			userToCheck[user] = struct{}{}
		}

		for user := range w.ACL.ReadWriteUsers {
			if existingUser[user] {
				continue
			}
			userToCheck[user] = struct{}{}
		}

		for user := range userToCheck {
			exist, err := v.identityQuerier.DoesUserExist(user)
			if err != nil {
				return nil, errors.WithMessagef(err, "error while validating access control definition")
			}

			if !exist {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the user [" + user + "] defined in the access control for the key [" + w.Key + "] does not exist",
				}, nil
			}

			existingUser[user] = true
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *dataTxValidator) validateFieldsInDataDeletes(
	dbName string,
	dataDeletes []*types.DataDelete,
	pendingOps *pendingOperations,
) (*types.ValidationInfo, error) {
	for _, d := range dataDeletes {
		if d == nil {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty entry in the delete list",
			}, nil
		}

		// as we collect the commits for a batch of transaction, we need to check whether the
		// key is already deleted by some other previous transaction in the block. Only if it
		// is not deleted by any previous transaction, we need to check whether the key exist
		// in the worldstate.
		if pendingOps.existDelete(dbName, d.Key) {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
				ReasonIfInvalid: "the key [" + d.Key + "] is already deleted by some previous transaction in the block",
			}, nil
		}

		val, metadata, err := v.db.Get(dbName, d.Key)
		if err != nil {
			return nil, errors.WithMessage(err, "error while validating delete entries")
		}
		if val == nil && metadata == nil {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [" + d.Key + "] does not exist in the database and hence, it cannot be deleted",
			}, nil
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func validateUniquenessInDataWritesAndDeletes(dataWrites []*types.DataWrite, dataDeletes []*types.DataDelete) *types.ValidationInfo {
	writeKeys := make(map[string]bool)
	deleteKeys := make(map[string]bool)

	for _, w := range dataWrites {
		if writeKeys[w.Key] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [" + w.Key + "] is duplicated in the write list. The keys in the write list must be unique",
			}
		}
		writeKeys[w.Key] = true
	}

	for _, d := range dataDeletes {
		switch {
		case deleteKeys[d.Key]:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [" + d.Key + "] is duplicated in the delete list. The keys in the delete list must be unique",
			}

		case writeKeys[d.Key]:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [" + d.Key + "] is being updated as well as deleted. Only one operation per key is allowed within a transaction",
			}
		}

		deleteKeys[d.Key] = true
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
}

func (v *dataTxValidator) validateACLOnDataReads(userIDs []string, dbName string, reads []*types.DataRead) (*types.ValidationInfo, error) {
	for _, r := range reads {
		acl, err := v.db.GetACL(dbName, r.Key)
		if err != nil {
			return nil, errors.WithMessagef(err, "error while validating ACL on the key [%s] in the reads", r.Key)
		}
		if acl == nil {
			continue
		}

		hasPerm := false
		for _, userID := range userIDs {
			if acl.ReadUsers[userID] || acl.ReadWriteUsers[userID] {
				// even if a single user has read permission, it is adequate
				hasPerm = true
				break
			}
		}

		if hasPerm {
			continue
		}

		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_NO_PERMISSION,
			ReasonIfInvalid: "none of the user in [" + strings.Join(userIDs, ",") + "] has a read permission on key [" + r.Key + "] present in the database [" + dbName + "]",
		}, nil
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *dataTxValidator) validateACLOnDataWrites(userIDs []string, dbName string, writes []*types.DataWrite) (*types.ValidationInfo, error) {
	var valRes *types.ValidationInfo
	var err error

	for _, w := range writes {
		valRes, err = v.validateACLForWriteOrDelete(userIDs, dbName, w.Key)
		if err != nil {
			return nil, err
		}

		if valRes.Flag != types.Flag_VALID {
			return valRes, nil
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *dataTxValidator) validateACLOnDataDeletes(userIDs []string, dbName string, deletes []*types.DataDelete) (*types.ValidationInfo, error) {
	var valRes *types.ValidationInfo
	var err error

	for _, d := range deletes {
		valRes, err = v.validateACLForWriteOrDelete(userIDs, dbName, d.Key)
		if err != nil {
			return nil, err
		}

		if valRes.Flag != types.Flag_VALID {
			return valRes, nil
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *dataTxValidator) validateACLForWriteOrDelete(userIDs []string, dbName, key string) (*types.ValidationInfo, error) {
	acl, err := v.db.GetACL(dbName, key)
	if err != nil {
		return nil, err
	}
	if acl == nil {
		return &types.ValidationInfo{
			Flag: types.Flag_VALID,
		}, nil
	}

	if len(acl.ReadWriteUsers) == 0 {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_NO_PERMISSION,
			ReasonIfInvalid: "no user can write or delete the key [" + key + "]",
		}, nil
	}

	switch acl.SignPolicyForWrite {
	case types.AccessControl_ANY:
		// even if a single user has a write permission, it is adequate
		hasPerm := false
		for _, userID := range userIDs {
			if acl.ReadWriteUsers[userID] {
				hasPerm = true
				break
			}
		}

		if !hasPerm {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "none of the user in [" + strings.Join(userIDs, ",") + "] has a write/delete permission on key [" + key + "] present in the database [" + dbName + "]",
			}, nil
		}

	case types.AccessControl_ALL:
		// only if all users present in the ACL list is included in the userIDs,
		// the operation is marked valid
		for targetUserID := range acl.ReadWriteUsers {
			found := false
			for _, userID := range userIDs {
				if targetUserID == userID {
					found = true
					break
				}
			}

			if !found {
				var targetUserIDs []string
				for userID := range acl.ReadWriteUsers {
					targetUserIDs = append(targetUserIDs, userID)
				}

				sort.Strings(targetUserIDs)
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_NO_PERMISSION,
					ReasonIfInvalid: "not all required users in [" + strings.Join(targetUserIDs, ",") + "] have signed the transaction to write/delete key [" + key + "] present in the database [" + dbName + "]",
				}, nil
			}
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *dataTxValidator) mvccValidation(dbName string, reads []*types.DataRead, pendingOps *pendingOperations) (*types.ValidationInfo, error) {
	for _, r := range reads {
		if pendingOps.exist(dbName, r.Key) {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
				ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [" + r.Key + "] in database [" + dbName + "]",
			}, nil
		}

		committedVersion, err := v.db.GetVersion(dbName, r.Key)
		if err != nil {
			return nil, err
		}
		if proto.Equal(r.Version, committedVersion) {
			continue
		}

		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
			ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the key [" + r.Key + "] in database [" + dbName + "] changed",
		}, nil
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}
