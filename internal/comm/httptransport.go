// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package comm

import (
	"fmt"
	"github.com/IBM-Blockchain/bcdb-server/config"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	etcd_types "go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft/raftpb"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

//go:generate counterfeiter -o mocks/consensus_listener.go --fake-name ConsensusListener . ConsensusListener

type ConsensusListener interface {
	rafthttp.Raft
}

type HTTPTransport struct {
	localConf *config.LocalConfiguration

	mutex             sync.Mutex
	consensusListener ConsensusListener
	clusterConfig     *types.ClusterConfig

	raftID uint64

	transport  *rafthttp.Transport
	httpServer *http.Server

	stopCh chan struct{} // signals HTTPTransport to shutdown
	doneCh chan struct{} // signals HTTPTransport shutdown complete

	logger *logger.SugarLogger
}

type Config struct {
	LocalConf *config.LocalConfiguration
	Logger    *logger.SugarLogger
}

func NewHTTPTransport(config *Config) *HTTPTransport {
	if config.LocalConf.Replication.TLS.Enabled {
		config.Logger.Panic("TLS not supported yet")
	}

	tr := &HTTPTransport{
		logger:    config.Logger,
		localConf: config.LocalConf,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	//TODO
	return tr
}

func (p *HTTPTransport) SetConsensusListener(l ConsensusListener) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.consensusListener != nil {
		return errors.New("ConsensusListener already set")
	}
	p.consensusListener = l

	return nil
}

//TODO dynamic re-config
func (p *HTTPTransport) UpdateClusterConfig(clusterConfig *types.ClusterConfig) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.clusterConfig != nil {
		return errors.New("dynamic re-config of http transport is not supported yet")
	}

	p.clusterConfig = clusterConfig
	return nil
}

func (p *HTTPTransport) Start() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.consensusListener == nil {
		p.logger.Panic("Must set ConsensusListener before Start()")
	}

	if p.clusterConfig == nil {
		p.logger.Panic("Must update ClusterConfig before Start()")
	}

	//TODO export to a config util
	var foundRaftID bool
	for _, member := range p.clusterConfig.ConsensusConfig.Members {
		if member.NodeId == p.localConf.Server.Identity.ID {
			p.raftID = member.RaftId
			foundRaftID = true
			break
		}
	}
	if !foundRaftID {
		return errors.Errorf("local NodeID '%s' is not in Consensus members: %v",
			p.localConf.Server.Identity.ID, p.clusterConfig.ConsensusConfig.Members)
	}

	netConf := p.localConf.Replication.Network
	addr := fmt.Sprintf("%s:%d", netConf.Address, netConf.Port)
	netListener, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Wrap(err, "error while creating a tcp listener")
	}

	p.transport = &rafthttp.Transport{
		Logger:      p.logger.Desugar(),
		ID:          etcd_types.ID(p.raftID),
		ClusterID:   0x1000, // TODO compute a ClusterID from the genesis block?
		Raft:        p.consensusListener,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(int(p.raftID))),
		ErrorC:      make(chan error),
	}

	if err = p.transport.Start(); err != nil {
		return errors.Wrapf(err, "failed to start rafthttp transport")
	}

	for _, peer := range p.clusterConfig.ConsensusConfig.Members {
		if peer.RaftId != p.raftID {
			p.transport.AddPeer(
				etcd_types.ID(peer.RaftId),
				[]string{fmt.Sprintf("http://%s:%d", peer.PeerHost, peer.PeerPort)}) //TODO unsecure for now, add TLS/https later
		}
	}

	p.httpServer = &http.Server{Handler: p.transport.Handler()}

	go p.servePeers(netListener)

	return nil
}

func (p *HTTPTransport) servePeers(l net.Listener) {
	p.logger.Infof("http transport starting to serve peers on: %s", l.Addr().String())
	err := p.httpServer.Serve(l)
	select {
	case <-p.stopCh:
		p.logger.Info("http transport stopping to server peers")
	default:
		p.logger.Errorf("http transport failed to serve peers (%v)", err)
	}
	close(p.doneCh)
}

func (p *HTTPTransport) Close() {
	p.logger.Info("closing http transport")
	close(p.stopCh)

	if err := p.httpServer.Close(); err != nil {
		p.logger.Errorf("http transport failed to close http server: %s", err)
	}

	p.transport.Stop()

	select {
	case <-p.doneCh:
		p.logger.Info("http transport closed")
	case <-time.After(10 * time.Second):
		p.logger.Info("http transport Close() timed-out waiting for http server to complete shutdown")
	}
}

func (p *HTTPTransport) SendConsensus(msgs []raftpb.Message) error {

	p.transport.Send(msgs)

	return nil
}
