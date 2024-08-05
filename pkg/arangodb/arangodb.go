package arangodb

import (
	"context"
	"encoding/json"

	driver "github.com/arangodb/go-driver"
	"github.com/cisco-open/jalapeno/topology/dbclient"
	"github.com/golang/glog"
	"github.com/jalapeno/ipv4-graph/pkg/kafkanotifier"
	"github.com/sbezverk/gobmp/pkg/bmp"
	"github.com/sbezverk/gobmp/pkg/message"
	"github.com/sbezverk/gobmp/pkg/tools"
)

type arangoDB struct {
	dbclient.DB
	*ArangoConn
	stop       chan struct{}
	lslink     driver.Collection
	lsprefix   driver.Collection
	graph      driver.Collection
	lsnodeExt  driver.Collection
	ebgpPeer   driver.Collection
	inetPrefix driver.Collection
	lsv4Graph  driver.Graph
	ipv4Graph  driver.Graph
	notifier   kafkanotifier.Event
}

// NewDBSrvClient returns an instance of a DB server client process
func NewDBSrvClient(arangoSrv, user, pass, dbname, lslink string, lsprefix string, lsnodeExt string,
	ebgpPeer string, inetPrefix string, lsv4Graph string, ipv4Graph string, notifier kafkanotifier.Event) (dbclient.Srv, error) {
	if err := tools.URLAddrValidation(arangoSrv); err != nil {
		return nil, err
	}
	arangoConn, err := NewArango(ArangoConfig{
		URL:      arangoSrv,
		User:     user,
		Password: pass,
		Database: dbname,
	})
	if err != nil {
		return nil, err
	}
	arango := &arangoDB{
		stop: make(chan struct{}),
	}
	arango.DB = arango
	arango.ArangoConn = arangoConn
	if notifier != nil {
		arango.notifier = notifier
	}

	// Check if ls_link edge collection exists, if not fail as Jalapeno topology is not running
	arango.lslink, err = arango.db.Collection(context.TODO(), lslink)
	if err != nil {
		return nil, err
	}

	// Check if ls_prefix collection exists, if not fail as Jalapeno topology is not running
	arango.lsprefix, err = arango.db.Collection(context.TODO(), lsprefix)
	if err != nil {
		return nil, err
	}

	//Check if ls_node_ext collection exists, if not fail as Jalapeno topology is not running
	arango.lsnodeExt, err = arango.db.Collection(context.TODO(), lsnodeExt)
	if err != nil {
		return nil, err
	}

	// Check if eBGP Peer collection exists, if not fail as Jalapeno topology is not running
	arango.ebgpPeer, err = arango.db.Collection(context.TODO(), ebgpPeer)
	if err != nil {
		return nil, err
	}

	// Check if inet_prefix collection exists, if not fail as Jalapeno topology is not running
	arango.inetPrefix, err = arango.db.Collection(context.TODO(), inetPrefix)
	if err != nil {
		return nil, err
	}

	// Check if original ls_topology collection exists, if not fail as Jalapeno topology is not running
	arango.lsv4Graph, err = arango.db.Graph(context.TODO(), lsv4Graph)
	glog.Infof("lsv4 topo collection found %+v", lsv4Graph)
	if err != nil {
		return nil, err
	}

	// check for ipv4 topology graph
	found, err := arango.db.GraphExists(context.TODO(), ipv4Graph)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Graph(context.TODO(), ipv4Graph)
		if err != nil {
			return nil, err
		}
		glog.Infof("found graph %s", c)

	} else {
		// create graph
		var edgeDefinition driver.EdgeDefinition
		edgeDefinition.Collection = "ipv4_graph"
		edgeDefinition.From = []string{"ls_node_extended", "ls_prefix", "ebgp_peer", "inet_prefix_v4"}
		edgeDefinition.To = []string{"ls_node_extended", "ls_prefix", "ebgp_peer", "inet_prefix_v4"}
		var options driver.CreateGraphOptions
		options.EdgeDefinitions = []driver.EdgeDefinition{edgeDefinition}

		glog.Infof("creating graph %s", ipv4Graph)
		arango.ipv4Graph, err = arango.db.CreateGraph(context.TODO(), ipv4Graph, &options)
		if err != nil {
			return nil, err
		}
	}

	// check if graph exists, if not fail as processor has failed to create graph
	arango.graph, err = arango.db.Collection(context.TODO(), "ipv4_graph")
	glog.Infof("checking collection %s", ipv4Graph)
	if err != nil {
		return nil, err
	}
	return arango, nil
}

func (a *arangoDB) Start() error {
	if err := a.loadEdge(); err != nil {
		return err
	}
	glog.Infof("Connected to arango database, starting monitor")

	return nil
}

func (a *arangoDB) Stop() error {
	close(a.stop)

	return nil
}

func (a *arangoDB) GetInterface() dbclient.DB {
	return a.DB
}

func (a *arangoDB) GetArangoDBInterface() *ArangoConn {
	return a.ArangoConn
}

func (a *arangoDB) StoreMessage(msgType dbclient.CollectionType, msg []byte) error {
	event := &kafkanotifier.EventMessage{}
	if err := json.Unmarshal(msg, event); err != nil {
		return err
	}
	glog.V(9).Infof("Received event from topology: %+v", *event)
	event.TopicType = msgType
	switch msgType {
	case bmp.LSLinkMsg:
		return a.lsLinkHandler(event)
	}
	switch msgType {
	case bmp.LSPrefixMsg:
		return a.lsprefixHandler(event)
	}
	switch msgType {
	case bmp.PeerStateChangeMsg:
		return a.peerHandler(event)
	}
	switch msgType {
	case bmp.UnicastPrefixV4Msg:
		return a.unicastprefixHandler(event)
	}
	return nil
}

func (a *arangoDB) loadEdge() error {
	ctx := context.TODO()
	glog.Infof("start processing vertices and edges")
	copy_ls_topo := "for l in lsv4_graph insert l in ipv4_graph options { overwrite: " + "\"update\"" + " } "
	cursor, err := a.db.Query(ctx, copy_ls_topo, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	lsprefix_query := "for l in ls_prefix filter l.mt_id_tlv.mt_id != 2 filter l.prefix_len < 26 return l"
	cursor, err = a.db.Query(ctx, lsprefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.LSPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		if err := a.processLSPrefixEdge(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	// egress / Inet peer
	bgp_query := "for l in peer let internal_asns = ( for n in ls_node return n.peer_asn ) " +
		"filter l.remote_asn not in internal_asns return l"
	cursor, err = a.db.Query(ctx, bgp_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.PeerStateChange
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		//glog.Infof("processing eBGP peers for ls_node: %s", p.Key)
		if err := a.processEgressPeer(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	epe_query := "for l in ls_link filter l.protocol_id == 7 filter l._key !like " + "\"%:%\"" + " return l"
	cursor, err = a.db.Query(ctx, epe_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.LSLink
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		//glog.Infof("get ipv4 epe_link: %s", p.Key)
		if err := a.processEPE(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	ibgp_prefix_query := "for l in " + a.lsnodeExt.Name() + " return l"
	cursor, err = a.db.Query(ctx, ibgp_prefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p LSNodeExt
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		glog.Infof("get ipv4 iBGP prefixes attach to lsnode: %s", p.Key)
		if err := a.processIBGPPrefix(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	// ebgp DC peers
	//peer2peer_query := "for l in peer return l"
	peer2peer_query := "for l in peer let internal_asns = ( for n in ls_node return n.peer_asn ) " +
		"filter l.remote_asn not in internal_asns filter l.remote_asn in 64512..65535 filter l._key !like " + "\"%:%\"" +
		" return l"
	cursor, err = a.db.Query(ctx, peer2peer_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.PeerStateChange
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		glog.Infof("connect private ASN eBGP peers in graph: %s", p.Key)
		if err := a.processPeerSession(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	inet_prefix_query := "for l in inet_prefix_v4 return l"
	cursor, err = a.db.Query(ctx, inet_prefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.UnicastPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		if err := a.processInetPrefix(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	ebgp_prefix_query := "for l in ebgp_prefix_v4 return l"
	cursor, err = a.db.Query(ctx, ebgp_prefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.UnicastPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		//glog.Infof("get ipv4 eBGP prefixes: %s", p.Key)
		if err := a.processeBgpPrefix(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	return nil
}
