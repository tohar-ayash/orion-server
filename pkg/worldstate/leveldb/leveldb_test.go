package leveldb

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

type testEnv struct {
	l       *LevelDB
	path    string
	cleanup func()
}

func newTestEnv(t *testing.T) *testEnv {
	dir, err := ioutil.TempDir("/tmp", "ledger")
	require.NoError(t, err)

	path := filepath.Join(dir, "leveldb")
	l, err := New(path)
	if err != nil {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("failed to remove %s, %v", dir, err)
		}
		t.Fatalf("failed to create leveldb with path %s", path)
	}

	cleanup := func() {
		if err := l.Close(); err != nil {
			t.Errorf("failed to close the database instance, %v", err)
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("failed to remove %s, %v", dir, err)
		}
	}

	require.Equal(t, path, l.dirPath)
	require.Len(t, l.dbs, len(systemDBs))
	for _, dbName := range systemDBs {
		require.NotNil(t, l.dbs[dbName])
	}

	return &testEnv{
		l:       l,
		path:    path,
		cleanup: cleanup,
	}
}

func TestCreateAndOpenDB(t *testing.T) {
	t.Parallel()

	t.Run("opening an non-existing database", func(t *testing.T) {
		t.Parallel()
		env := newTestEnv(t)
		defer env.cleanup()
		l := env.l
		require.Contains(t, l.Open("db1").Error(), "database db1 does not exist")
		exists, err := fileExists(filepath.Join(env.path, "db1"))
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("creating and opening a database", func(t *testing.T) {
		t.Parallel()
		env := newTestEnv(t)
		defer env.cleanup()
		l := env.l
		require.NoError(t, l.Create("db1"))
		require.DirExists(t, filepath.Join(env.path, "db1"))
		require.NotEmpty(t, l.dbs)
		require.NotNil(t, l.dbs["db1"])
		require.NoError(t, l.Open("db1"))
	})
}

func TestCommitAndGet(t *testing.T) {
	t.Parallel()

	setupWithNoData := func(l *LevelDB) {
		require.NoError(t, l.Create("db1"))
		require.NoError(t, l.Create("db2"))
	}

	setupWithData := func(l *LevelDB) (map[string]*types.Value, map[string]*types.Value) {
		db1val1 := &types.Value{
			Value: []byte("db1-value1"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    1,
				},
			},
		}
		db1val2 := &types.Value{
			Value: []byte("db1-value2"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    2,
				},
			},
		}
		db2val1 := &types.Value{
			Value: []byte("db2-value1"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    3,
				},
			},
		}
		db2val2 := &types.Value{
			Value: []byte("db2-value2"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    4,
				},
			},
		}
		dbsUpdates := []*worldstate.DBUpdates{
			{
				DBName: "db1",
				Writes: []*worldstate.KV{
					{
						Key:   "db1-key1",
						Value: db1val1,
					},
					{
						Key:   "db1-key2",
						Value: db1val2,
					},
				},
			},
			{
				DBName: "db2",
				Writes: []*worldstate.KV{
					{
						Key:   "db2-key1",
						Value: db2val1,
					},
					{
						Key:   "db2-key2",
						Value: db2val2,
					},
				},
			},
		}

		require.NoError(t, l.Create("db1"))
		require.NoError(t, l.Create("db2"))
		require.NoError(t, l.Commit(dbsUpdates))

		db1KVs := map[string]*types.Value{
			"db1-key1": db1val1,
			"db1-key2": db1val2,
		}
		db2KVs := map[string]*types.Value{
			"db2-key1": db2val1,
			"db2-key2": db2val2,
		}
		return db1KVs, db2KVs
	}

	t.Run("Get() and GetVersion() on empty databases", func(t *testing.T) {
		t.Parallel()
		env := newTestEnv(t)
		defer env.cleanup()
		l := env.l
		setupWithNoData(l)

		val, err := l.Get("db1", "db1-key1")
		require.NoError(t, err)
		require.Nil(t, val)

		val, err = l.Get("db2", "db2-key1")
		require.NoError(t, err)
		require.Nil(t, val)

		ver, err := l.GetVersion("db1", "db1-key1")
		require.NoError(t, err)
		require.Nil(t, ver)

		ver, err = l.GetVersion("db2", "db2-key1")
		require.NoError(t, err)
		require.Nil(t, ver)
	})

	// Scenario-2: For both databases (db1, db2), create
	// two keys per database (db1-key1, db1-key2 for db1) and
	// (db2-key1, db2-key2 for db2) and commit them.
	t.Run("Get() and GetVersion() on non-empty databases", func(t *testing.T) {
		t.Parallel()
		env := newTestEnv(t)
		defer env.cleanup()
		l := env.l
		db1KVs, db2KVs := setupWithData(l)

		for key, expectedVal := range db1KVs {
			val, err := l.Get("db1", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedVal, val))

			ver, err := l.GetVersion("db1", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedVal.GetMetadata().GetVersion(), ver))
		}

		for key, expectedVal := range db2KVs {
			val, err := l.Get("db2", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedVal, val))

			ver, err := l.GetVersion("db2", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedVal.GetMetadata().GetVersion(), ver))
		}
	})

	// Scenario-2: For both databases (db1, db2), update
	// the first key and delete the second key.
	// (db1-key1 is updated, db1-key2 is deleted for db1) and
	// (db2-key1 is updated, db2-key2 is deleted for db2) and
	// commit them.
	t.Run("Commit() and Get() on non-empty databases", func(t *testing.T) {
		t.Parallel()
		env := newTestEnv(t)
		defer env.cleanup()
		l := env.l
		db1KVs, db2KVs := setupWithData(l)

		db1val1new := &types.Value{
			Value: []byte("db1-value1-new"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 2,
					TxNum:    1,
				},
			},
		}
		db2val1new := &types.Value{
			Value: []byte("db2-value1-new"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 2,
					TxNum:    2,
				},
			},
		}
		dbsUpdates := []*worldstate.DBUpdates{
			{
				DBName: "db1",
				Writes: []*worldstate.KV{
					{
						Key:   "db1-key1",
						Value: db1val1new,
					},
				},
				Deletes: []string{"db1-key2"},
			},
			{
				DBName: "db2",
				Writes: []*worldstate.KV{
					{
						Key:   "db2-key1",
						Value: db2val1new,
					},
				},
				Deletes: []string{"db2-key2"},
			},
		}

		require.NoError(t, l.Commit(dbsUpdates))

		db1KVs["db1-key1"] = db1val1new
		db1KVs["db1-key2"] = nil
		for key, expectedVal := range db1KVs {
			val, err := l.Get("db1", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedVal, val))

			ver, err := l.GetVersion("db1", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedVal.GetMetadata().GetVersion(), ver))
		}

		db2KVs["db2-key1"] = db2val1new
		db2KVs["db2-key2"] = nil
		for key, expectedVal := range db2KVs {
			val, err := l.Get("db2", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedVal, val))

			ver, err := l.GetVersion("db2", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedVal.GetMetadata().GetVersion(), ver))
		}
	})
}

func TestNewLevelDB(t *testing.T) {
	t.Parallel()

	dir, err := ioutil.TempDir("/tmp", "ledger")
	require.NoError(t, err)
	levelPath := filepath.Join(dir, "leveldb")
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
		require.NoError(t, os.RemoveAll(levelPath))
	}()

	t.Run("open-new-databases", func(t *testing.T) {
		l, err := New(levelPath)
		require.NoError(t, err)
		require.NoError(t, l.Open(worldstate.UsersDBName))

		userDBs := []string{"db1", "db2", "db3", "db4"}
		for _, dbName := range userDBs {
			require.NoError(t, l.Create(dbName))
		}

		require.Len(t, l.dbs, 4+len(systemDBs))

		for _, dbName := range systemDBs {
			require.NoError(t, l.Open(dbName))
		}

		require.NoError(t, l.Close())
	})

	t.Run("reopen-old-databases", func(t *testing.T) {
		l, err := New(levelPath)
		require.NoError(t, err)
		require.Len(t, l.dbs, 4+len(systemDBs))

		for _, dbName := range systemDBs {
			require.NoError(t, l.Open(dbName))
		}

		userDBs := []string{"db1", "db2", "db3", "db4"}
		for _, dbName := range userDBs {
			require.NoError(t, l.Open(dbName))
		}
	})
}
