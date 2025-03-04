// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockprocessor

import (
	"github.com/IBM-Blockchain/bcdb-server/internal/identity"
	"github.com/IBM-Blockchain/bcdb-server/pkg/cryptoservice"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/pkg/errors"
)

// validator validates the each transaction read set present in a
// block against the committed version to ensure the requested
// isolation level
type validator struct {
	configTxValidator    *configTxValidator
	dbAdminTxValidator   *dbAdminTxValidator
	userAdminTxValidator *userAdminTxValidator
	dataTxValidator      *dataTxValidator
	signValidator        *txSigValidator
	logger               *logger.SugarLogger
}

// newValidator creates a new validator
func newValidator(conf *Config) *validator {
	idQuerier := identity.NewQuerier(conf.DB)
	txSigValidator := &txSigValidator{
		sigVerifier: cryptoservice.NewVerifier(idQuerier, conf.Logger),
		logger:      conf.Logger,
	}

	return &validator{
		configTxValidator: &configTxValidator{
			db:              conf.DB,
			identityQuerier: idQuerier,
			sigValidator:    txSigValidator,
			logger:          conf.Logger,
		},

		dbAdminTxValidator: &dbAdminTxValidator{
			db:              conf.DB,
			identityQuerier: idQuerier,
			sigValidator:    txSigValidator,
			logger:          conf.Logger,
		},

		userAdminTxValidator: &userAdminTxValidator{
			db:              conf.DB,
			identityQuerier: idQuerier,
			sigValidator:    txSigValidator,
			logger:          conf.Logger,
		},

		dataTxValidator: &dataTxValidator{
			db:              conf.DB,
			identityQuerier: idQuerier,
			sigValidator:    txSigValidator,
			logger:          conf.Logger,
		},

		signValidator: txSigValidator,

		logger: conf.Logger,
	}
}

// validateBlock validates each transaction present in the block to ensure
// the request isolation level
func (v *validator) validateBlock(block *types.Block) ([]*types.ValidationInfo, error) {
	if block.Header.BaseHeader.Number == 1 {
		// for the genesis block, which is created by the node itself, we cannot
		// do a regular validation but we still needs to validate the entries
		configTx := block.GetConfigTxEnvelope().Payload

		r, caCertCollection := validateCAConfig(configTx.NewConfig.CertAuthConfig)
		if r.Flag != types.Flag_VALID {
			return nil, errors.Errorf("genesis block cannot be invalid: reason for invalidation [%s]", r.ReasonIfInvalid)
		}

		if r := validateNodeConfig(configTx.NewConfig.Nodes, caCertCollection); r.Flag != types.Flag_VALID {
			return nil, errors.Errorf("genesis block cannot be invalid: reason for invalidation [%s]", r.ReasonIfInvalid)
		}

		if r := validateAdminConfig(configTx.NewConfig.Admins, caCertCollection); r.Flag != types.Flag_VALID {
			return nil, errors.Errorf("genesis block cannot be invalid: reason for invalidation [%s]", r.ReasonIfInvalid)
		}

		if r := validateConsensusConfig(configTx.NewConfig.ConsensusConfig); r.Flag != types.Flag_VALID {
			return nil, errors.Errorf("genesis block cannot be invalid: reason for invalidation [%s]", r.ReasonIfInvalid)
		}

		if r := validateMembersNodesMatch(configTx.NewConfig.ConsensusConfig.Members, configTx.NewConfig.Nodes); r.Flag != types.Flag_VALID {
			return nil, errors.Errorf("genesis block cannot be invalid: reason for invalidation [%s]", r.ReasonIfInvalid)
		}

		return []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
		}, nil
	}

	switch block.Payload.(type) {
	case *types.Block_DataTxEnvelopes:
		dataTxEnvs := block.GetDataTxEnvelopes().Envelopes
		valInfo := make([]*types.ValidationInfo, len(dataTxEnvs))
		pendingOps := newPendingOperations()

		for txNum, txEnv := range dataTxEnvs {
			valRes, err := v.dataTxValidator.validate(txEnv, pendingOps)
			if err != nil {
				return nil, errors.WithMessage(err, "error while validating data transaction")
			}

			valInfo[txNum] = valRes
			if valRes.Flag != types.Flag_VALID {
				v.logger.Debugf("data transaction [%v] is invalid due to [%s]", txEnv.Payload, valRes.ReasonIfInvalid)
				continue
			}

			for _, ops := range txEnv.Payload.DBOperations {
				for _, w := range ops.DataWrites {
					pendingOps.addWrite(ops.DBName, w.Key)
				}

				for _, d := range ops.DataDeletes {
					pendingOps.addDelete(ops.DBName, d.Key)
				}
			}
		}

		return valInfo, nil

	case *types.Block_UserAdministrationTxEnvelope:
		userTxEnv := block.GetUserAdministrationTxEnvelope()
		valRes, err := v.userAdminTxValidator.validate(userTxEnv)
		if err != nil {
			return nil, errors.WithMessage(err, "error while validating user administrative transaction")
		}

		if valRes.Flag != types.Flag_VALID {
			v.logger.Debugf("user administration transaction [%v] is invalid due to [%s]", userTxEnv.Payload, valRes.ReasonIfInvalid)
		}

		return []*types.ValidationInfo{
			valRes,
		}, nil

	case *types.Block_DBAdministrationTxEnvelope:
		dbTxEnv := block.GetDBAdministrationTxEnvelope()
		valRes, err := v.dbAdminTxValidator.validate(dbTxEnv)
		if err != nil {
			return nil, errors.WithMessage(err, "error while validating db administrative transaction")
		}

		if valRes.Flag != types.Flag_VALID {
			v.logger.Debugf("database administration transaction [%v] is invalid due to [%s]", dbTxEnv.Payload, valRes.ReasonIfInvalid)
		}

		return []*types.ValidationInfo{
			valRes,
		}, nil

	case *types.Block_ConfigTxEnvelope:
		configTxEnv := block.GetConfigTxEnvelope()
		valRes, err := v.configTxValidator.validate(configTxEnv)
		if err != nil {
			return nil, errors.WithMessage(err, "error while validating config transaction")
		}

		if valRes.Flag != types.Flag_VALID {
			v.logger.Debugf("cluster config transaction [%v] is invalid due to [%s]", configTxEnv, valRes.ReasonIfInvalid)
		}

		return []*types.ValidationInfo{
			valRes,
		}, nil

	default:
		return nil, errors.Errorf("unexpected transaction envelope in the block")
	}
}

type pendingOperations struct {
	pendingWrites  map[string]bool
	pendingDeletes map[string]bool
}

func newPendingOperations() *pendingOperations {
	return &pendingOperations{
		pendingWrites:  make(map[string]bool),
		pendingDeletes: make(map[string]bool),
	}
}

func (p *pendingOperations) addWrite(dbName, key string) {
	ckey := constructCompositeKey(dbName, key)
	p.pendingWrites[ckey] = true
}

func (p *pendingOperations) addDelete(dbName, key string) {
	ckey := constructCompositeKey(dbName, key)
	p.pendingDeletes[ckey] = true
}

func (p *pendingOperations) existDelete(dbName, key string) bool {
	ckey := constructCompositeKey(dbName, key)
	return p.pendingDeletes[ckey]
}

func (p *pendingOperations) exist(dbName, key string) bool {
	ckey := constructCompositeKey(dbName, key)
	return p.pendingWrites[ckey] || p.pendingDeletes[ckey]
}

func constructCompositeKey(dbName, key string) string {
	return dbName + "~" + key
}
