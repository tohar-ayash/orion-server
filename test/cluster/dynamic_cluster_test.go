// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"encoding/pem"
	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"io/ioutil"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func createNewServer(c *setup.Cluster, conf *setup.Config, serverNum int) (*setup.Server, *types.PeerConfig, *types.NodeConfig, error) {
	newServer, err := setup.NewServer(uint64(3), conf.TestDirAbsolutePath, conf.BaseNodePort, conf.BasePeerPort,
		conf.CheckRedirectFunc, c.GetLogger(), "join")
	if err != nil {
		return nil, nil, nil, err
	}
	clusterCaPrivKey, clusterRootCAPemCert := c.GetKeyAndCA()
	decca, _ := pem.Decode(clusterRootCAPemCert)
	newServer.CreateCryptoMaterials(clusterRootCAPemCert, clusterCaPrivKey)
	if err != nil {
		return nil, nil, nil, err
	}
	server0 := c.Servers[0]
	newServer.SetAdmin(server0.AdminID(), server0.AdminCertPath(), server0.AdminKeyPath(), server0.AdminSigner())

	newPeer := &types.PeerConfig{
		NodeId:   "node-" + strconv.Itoa(serverNum+1),
		RaftId:   uint64(serverNum + 1),
		PeerHost: "127.0.0.1",
		PeerPort: conf.BasePeerPort + uint32(serverNum),
	}

	newNode := &types.NodeConfig{
		Id:          "node-" + strconv.Itoa(serverNum+1),
		Address:     "127.0.0.1",
		Port:        conf.BaseNodePort + uint32(serverNum),
		Certificate: decca.Bytes,
	}

	return newServer, newPeer, newNode, nil
}

// Scenario:
// - Start a 3 node cluster
// - add a server (node-4)
// - put the config tx block as a join block in the node4 config folder, and generate a config with bootstrap method "join"
// - start the new server
func TestBasicAddServer(t *testing.T) {
	dir, err := ioutil.TempDir("", "int-test")
	require.NoError(t, err)

	nPort, pPort := getPorts(3)
	setupConfig := &setup.Config{
		NumberOfServers:     3,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		CheckRedirectFunc: func(req *http.Request, via []*http.Request) error {
			return errors.Errorf("Redirect blocked in test client: url: '%s', referrer: '%s', #via: %d", req.URL, req.Referer(), len(via))
		},
	}
	c, err := setup.NewCluster(setupConfig)
	require.NoError(t, err)
	defer c.ShutdownAndCleanup()

	require.NoError(t, c.Start())

	leaderIndex := -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0, 1, 2)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)
	leaderServer := c.Servers[leaderIndex]

	statusResponseEnvelope, err := leaderServer.QueryClusterStatus(t)
	require.Equal(t, 3, len(statusResponseEnvelope.GetResponse().Nodes))
	require.Equal(t, 3, len(statusResponseEnvelope.GetResponse().Active))

	//get current cluster config
	configEnv, err := leaderServer.QueryConfig(t)
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig := configEnv.GetResponse().GetConfig()

	// create new server with method = "join"
	newServer, newPeer, newNode, err := createNewServer(c, setupConfig, 3)
	require.NoError(t, err)
	require.NotNil(t, newServer)
	require.NotNil(t, newPeer)
	require.NotNil(t, newNode)
	require.NoError(t, newServer.CreateConfigFile())
	newConfig.ConsensusConfig.Members = append(newConfig.ConsensusConfig.Members, newPeer)
	newConfig.Nodes = append(newConfig.Nodes, newNode)

	// add a server (node-4) with config-tx
	txID, rcpt, err := c.Servers[leaderIndex].SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "admin")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	statusResponseEnvelope, err = c.Servers[leaderIndex].QueryClusterStatus(t)
	require.Equal(t, 4, len(statusResponseEnvelope.GetResponse().Nodes))
	require.Equal(t, 3, len(statusResponseEnvelope.GetResponse().Active))

	configBlockResponseEnvelope, err := c.Servers[leaderIndex].QueryConfigBlockStatus(t)
	require.NoError(t, err)
	require.NotNil(t, configBlockResponseEnvelope)

	err = ioutil.WriteFile(newServer.BootstrapFilePath(), configBlockResponseEnvelope.Response.Block, 0666)
	require.NoError(t, err)

	config, err := config.Read(newServer.ConfigFilePath())
	require.NoError(t, err)
	require.NotNil(t, config.LocalConfig)
	require.Nil(t, config.SharedConfig)
	require.NotNil(t, config.JoinBlock)

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 2, 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)

	t.Logf("starting node-4")
	require.NoError(t, c.StartServer(newServer))
	c.AddServerToServers(newServer)

	leaderIndex = -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0, 1, 2, 3)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	statusResponseEnvelope, err = c.Servers[leaderIndex].QueryClusterStatus(t)
	require.Equal(t, 4, len(statusResponseEnvelope.GetResponse().Nodes))
	require.Equal(t, 4, len(statusResponseEnvelope.GetResponse().Active))
}
