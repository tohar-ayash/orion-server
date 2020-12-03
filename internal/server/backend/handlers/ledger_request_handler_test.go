package handlers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/internal/server/backend"
	"github.ibm.com/blockchaindb/server/internal/server/backend/mocks"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func TestBlockQuery(t *testing.T) {
	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.GetBlockResponseEnvelope) backend.DB
		expectedResponse   *types.GetBlockResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "valid get header request",
			expectedResponse: &types.GetBlockResponseEnvelope{
				Signature: []byte{0, 0, 0},
				Payload: &types.GetBlockResponse{
					Header: &types.ResponseHeader{
						NodeID: "testNodeID",
					},
					BlockHeader: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number: 1,
						},
					},
				},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerBlock(1), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetBlockQuery{UserID: submittingUserName, BlockNumber: 1})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetBlockResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetBlockHeader", submittingUserName, uint64(1)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:             "user doesn't exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerBlock(1), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetBlockQuery{UserID: submittingUserName, BlockNumber: 1})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetBlockResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"))
				return db
			},
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name: "no block exist",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerBlock(1), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetBlockQuery{UserID: submittingUserName, BlockNumber: 1})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetBlockResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetBlockHeader", submittingUserName, uint64(1)).Return(nil, errors.New("no such block"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError, // TODO deal with 404 not found, it's not a 5xx
			expectedErr:        "error while processing 'GET /ledger/block/1' because no such block",
		},
		{
			name:             "invalid block id",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.LedgerEndpoint+path.Join("block", "block1"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetBlockResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).
					Return(true, nil)
				db.On("GetBlockHeader", submittingUserName, uint64(1)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "query error - bad or missing block number literal blockId strconv.ParseUint: parsing \"block1\": invalid syntax",
		},
	}

	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			req, err := tt.requestFactory()
			require.NoError(t, err)
			require.NotNil(t, req)

			db := tt.dbMockFactory(tt.expectedResponse)
			rr := httptest.NewRecorder()
			handler := NewLedgerRequestHandler(db, logger)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedStatusCode, rr.Code)
			if tt.expectedStatusCode != http.StatusOK {
				respErr := &ResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}

			if tt.expectedResponse != nil {
				res := &types.GetBlockResponseEnvelope{}
				err = json.NewDecoder(rr.Body).Decode(res)
				require.NoError(t, err)
				require.Equal(t, tt.expectedResponse, res)
				//TODO verify signature on response
			}
		})
	}
}

func TestPathQuery(t *testing.T) {
	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.GetLedgerPathResponseEnvelope) backend.DB
		expectedResponse   *types.GetLedgerPathResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "valid get path request",
			expectedResponse: &types.GetLedgerPathResponseEnvelope{
				Signature: []byte{0, 0, 0},
				Payload: &types.GetLedgerPathResponse{
					Header: &types.ResponseHeader{
						NodeID: "testNodeID",
					},
					BlockHeaders: []*types.BlockHeader{
						{
							BaseHeader: &types.BlockHeaderBase{
								Number: 1,
							},
						},
						{
							BaseHeader: &types.BlockHeaderBase{
								Number: 2,
							},
						},
					},
				},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerPath(1, 2), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetLedgerPathQuery{
					UserID:           submittingUserName,
					StartBlockNumber: 1,
					EndBlockNumber:   2,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetLedgerPathResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetLedgerPath", submittingUserName, uint64(1), uint64(2)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:             "user doesn't exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerPath(1, 2), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetLedgerPathQuery{
					UserID:           submittingUserName,
					StartBlockNumber: 1,
					EndBlockNumber:   2,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetLedgerPathResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"))
				db.On("GetLedgerPath", submittingUserName, uint64(1), uint64(2)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name:             "no path exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerPath(1, 2), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetLedgerPathQuery{
					UserID:           submittingUserName,
					StartBlockNumber: 1,
					EndBlockNumber:   2,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetLedgerPathResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetLedgerPath", submittingUserName, uint64(1), uint64(2)).Return(response, errors.Errorf("can't find path in blocks skip list between 2 1"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET /ledger/path?start=1&end=2' because can't find path in blocks skip list between 2 1",
		},
		{
			name:             "wrong url, endId not exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.LedgerEndpoint+fmt.Sprintf("path?start=%s", "1"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetLedgerPathResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).
					Return(true, nil)
				db.On("GetLedgerPath", submittingUserName, uint64(1), uint64(2)).Return(response, errors.Errorf("can't find path in blocks skip list between 2 1"))
				return db
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "query error - bad or missing start/end block number",
		},
	}

	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			req, err := tt.requestFactory()
			require.NoError(t, err)
			require.NotNil(t, req)

			db := tt.dbMockFactory(tt.expectedResponse)
			rr := httptest.NewRecorder()
			handler := NewLedgerRequestHandler(db, logger)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedStatusCode, rr.Code)
			if tt.expectedStatusCode != http.StatusOK {
				respErr := &ResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}

			if tt.expectedResponse != nil {
				res := &types.GetLedgerPathResponseEnvelope{}
				rr.Body.Bytes()
				err = json.NewDecoder(rr.Body).Decode(res)
				require.NoError(t, err)
				require.Equal(t, tt.expectedResponse, res)
			}
		})
	}
}

func TestProofQuery(t *testing.T) {
	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.GetTxProofResponseEnvelope) backend.DB
		expectedResponse   *types.GetTxProofResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "valid get path request",
			expectedResponse: &types.GetTxProofResponseEnvelope{
				Signature: []byte{0, 0, 0},
				Payload: &types.GetTxProofResponse{
					Header: &types.ResponseHeader{
						NodeID: "testNodeID",
					},
					Hashes: [][]byte{[]byte("hash1"), []byte("hash2")},
				},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLTxProof(2, 1), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetTxProofQuery{
					UserID:      submittingUserName,
					BlockNumber: 2,
					TxIndex:     1,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetTxProofResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetTxProof", submittingUserName, uint64(2), uint64(1)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:             "user doesn't exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLTxProof(2, 1), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetTxProofQuery{
					UserID:      submittingUserName,
					BlockNumber: 2,
					TxIndex:     1,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetTxProofResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"))
				db.On("GetTxProof", submittingUserName, uint64(2), uint64(1)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name:             "no tx exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLTxProof(2, 2), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetTxProofQuery{
					UserID:      submittingUserName,
					BlockNumber: 2,
					TxIndex:     2,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetTxProofResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetTxProof", submittingUserName, uint64(2), uint64(2)).Return(response, errors.Errorf("node with index 2 is not part of merkle tree (1, 1)"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET /ledger/proof/2?idx=2' because node with index 2 is not part of merkle tree (1, 1)",
		},
		{
			name:             "wrong url, idx not exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, path.Join(constants.LedgerEndpoint, "proof", "2"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetTxProofResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).
					Return(true, nil)
				db.On("GetTxProof", submittingUserName, uint64(2), uint64(2)).Return(response, errors.Errorf("query error - bad or missing tx index"))
				return db
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "query error - bad or missing tx index",
		},
	}

	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			req, err := tt.requestFactory()
			require.NoError(t, err)
			require.NotNil(t, req)

			db := tt.dbMockFactory(tt.expectedResponse)
			rr := httptest.NewRecorder()
			handler := NewLedgerRequestHandler(db, logger)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedStatusCode, rr.Code)
			if tt.expectedStatusCode != http.StatusOK {
				respErr := &ResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}

			if tt.expectedResponse != nil {
				res := &types.GetTxProofResponseEnvelope{}
				rr.Body.Bytes()
				err = json.NewDecoder(rr.Body).Decode(res)
				require.NoError(t, err)
				require.Equal(t, tt.expectedResponse, res)
			}
		})
	}
}

func TestTxReceiptQuery(t *testing.T) {
	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.GetTxReceiptResponseEnvelope) backend.DB
		expectedResponse   *types.GetTxReceiptResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "valid get receipt request",
			expectedResponse: &types.GetTxReceiptResponseEnvelope{
				Signature: []byte{0, 0, 0},
				Payload: &types.GetTxReceiptResponse{
					Header: &types.ResponseHeader{
						NodeID: "testNodeID",
					},
					Receipt: &types.TxReceipt{
						Header: &types.BlockHeader{
							BaseHeader: &types.BlockHeaderBase{
								Number: 2,
							},
						},
						TxIndex: 1,
					},
				},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetTransactionReceipt("tx1"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetTxReceiptQuery{
					UserID: submittingUserName,
					TxID:   "tx1",
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetTxReceiptResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetTxReceipt", submittingUserName, "tx1").Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:             "user doesn't exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetTransactionReceipt("tx1"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetTxReceiptQuery{
					UserID: submittingUserName,
					TxID:   "tx1",
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetTxReceiptResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"))
				db.On("GetTxReceipt", submittingUserName, "tx1").Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name:             "tx not exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetTransactionReceipt("tx1"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetTxReceiptQuery{
					UserID: submittingUserName,
					TxID:   "tx1",
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetTxReceiptResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetTxReceipt", submittingUserName, "tx1").Return(response, errors.Errorf("tx not found"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET /ledger/tx/receipt/tx1' because tx not found",
		},
	}

	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			req, err := tt.requestFactory()
			require.NoError(t, err)
			require.NotNil(t, req)

			db := tt.dbMockFactory(tt.expectedResponse)
			rr := httptest.NewRecorder()
			handler := NewLedgerRequestHandler(db, logger)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedStatusCode, rr.Code)
			if tt.expectedStatusCode != http.StatusOK {
				respErr := &ResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}

			if tt.expectedResponse != nil {
				res := &types.GetTxReceiptResponseEnvelope{}
				rr.Body.Bytes()
				err = json.NewDecoder(rr.Body).Decode(res)
				require.NoError(t, err)
				require.Equal(t, tt.expectedResponse, res)
			}
		})
	}
}
