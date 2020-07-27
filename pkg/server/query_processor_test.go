package server

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/worldstate/leveldb"
)

type queryProcessorTestEnv struct {
	db      *leveldb.LevelDB
	q       *queryProcessor
	cleanup func(t *testing.T)
}

func newQueryProcessorTestEnv(t *testing.T) *queryProcessorTestEnv {
	path, err := ioutil.TempDir("/tmp", "queryProcessor")
	require.NoError(t, err)

	cleanup := func(t *testing.T) {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("failed to remove %s due to %v", path, err)
		}
	}

	db, err := leveldb.New(path)
	if err != nil {
		cleanup(t)
		t.Fatalf("failed to create a new leveldb instance, %v", err)
	}

	return &queryProcessorTestEnv{
		db:      db,
		q:       newQueryProcessor(db),
		cleanup: cleanup,
	}
}

func TestGetStatus(t *testing.T) {
	t.Run("GetStatus-Returns-Status", func(t *testing.T) {
		env := newQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		require.NoError(t, env.db.Create("test-db"))

		testCases := []struct {
			dbName  string
			isExist bool
		}{
			{
				dbName:  "test-db",
				isExist: true,
			},
			{
				dbName:  "random",
				isExist: false,
			},
		}

		for _, testCase := range testCases {
			req := &types.GetStatusQueryEnvelope{
				Payload: &types.GetStatusQuery{
					UserID: "testUser",
					DBName: testCase.dbName,
				},
				Signature: []byte("signature"),
			}
			status, err := env.q.GetStatus(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, testCase.isExist, status.Payload.Exist)
		}
	})

	t.Run("GetStatus-Returns-Error", func(t *testing.T) {
		env := newQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		testCases := []struct {
			request       *types.GetStatusQueryEnvelope
			expectedError string
		}{
			{
				request:       nil,
				expectedError: "`GetStatusQueryEnvelope` is nil",
			},
			{
				request: &types.GetStatusQueryEnvelope{
					Payload: nil,
				},
				expectedError: "`Payload` in `GetStatusQueryEnvelope` is nil",
			},
			{
				request: &types.GetStatusQueryEnvelope{
					Payload: &types.GetStatusQuery{
						UserID: "",
					},
				},
				expectedError: "`UserID` is not set in `Payload`",
			},
		}

		for _, testCase := range testCases {
			status, err := env.q.GetStatus(context.Background(), testCase.request)
			require.Contains(t, err.Error(), testCase.expectedError)
			require.Nil(t, status)
		}
	})
}

func TestGetState(t *testing.T) {
	t.Run("GetState-Returns-State", func(t *testing.T) {
		env := newQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		require.NoError(t, env.db.Create("test-db"))
		val1 := &types.Value{
			Value: []byte("value1"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    1,
				},
			},
		}
		dbsUpdates := []*worldstate.DBUpdates{
			{
				DBName: "test-db",
				Writes: []*worldstate.KV{
					{
						Key:   "key1",
						Value: val1,
					},
				},
			},
		}
		require.NoError(t, env.db.Commit(dbsUpdates))

		testCases := []struct {
			key           string
			expectedValue *types.Value
		}{
			{
				key:           "key1",
				expectedValue: val1,
			},
			{
				key:           "not-present",
				expectedValue: nil,
			},
		}

		for _, testCase := range testCases {
			req := &types.GetStateQueryEnvelope{
				Payload: &types.GetStateQuery{
					UserID: "testUser",
					DBName: "test-db",
					Key:    testCase.key,
				},
				Signature: []byte("signature"),
			}

			val, err := env.q.GetState(context.Background(), req)
			require.NoError(t, err)
			require.True(t, proto.Equal(testCase.expectedValue, val.Payload.Value))
		}
	})

	t.Run("GetState-Returns-Error", func(t *testing.T) {
		env := newQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		testCases := []struct {
			request       *types.GetStateQueryEnvelope
			expectedError string
		}{
			{
				request:       nil,
				expectedError: "`GetStateQueryEnvelope` is nil",
			},
			{
				request: &types.GetStateQueryEnvelope{
					Payload: nil,
				},
				expectedError: "`Payload` in `GetStateQueryEnvelope` is nil",
			},
			{
				request: &types.GetStateQueryEnvelope{
					Payload: &types.GetStateQuery{
						UserID: "",
					},
				},
				expectedError: "`UserID` is not set in `Payload`",
			},
		}

		for _, testCase := range testCases {
			state, err := env.q.GetState(context.Background(), testCase.request)
			require.Contains(t, err.Error(), testCase.expectedError)
			require.Nil(t, state)
		}
	})
}
