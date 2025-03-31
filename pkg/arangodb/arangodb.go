package arangodb

import (
	"context"
	"encoding/json"
	"fmt"

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
	stop         chan struct{}
	graph        driver.Collection
	peer         driver.Collection
	bgpNode      driver.Collection
	ebgpprefixV4 driver.Collection
	inetprefixV4 driver.Collection
	ibgpprefixV4 driver.Collection
	ipv4Graph    driver.Graph
	notifier     kafkanotifier.Event
}

// NewDBSrvClient returns an instance of a DB server client process
func NewDBSrvClient(arangoSrv, user, pass, dbname, peer, bgpNode, unicastprefixV4,
	ebgpprefixV4, inetprefixV4, ibgpprefixV4, ipv4Graph string,
	notifier kafkanotifier.Event) (dbclient.Srv, error) {
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

	// check for bgp_node collection
	found, err := arango.db.CollectionExists(context.TODO(), bgpNode)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), bgpNode)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}

	// check for ebgp4 prefix collection
	found, err = arango.db.CollectionExists(context.TODO(), ebgpprefixV4)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), ebgpprefixV4)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}

	// check for inet4 prefix collection
	found, err = arango.db.CollectionExists(context.TODO(), inetprefixV4)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), inetprefixV4)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}

	// check for ibgp4 prefix collection
	found, err = arango.db.CollectionExists(context.TODO(), ibgpprefixV4)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), ibgpprefixV4)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}

	glog.Infof("checking collections")
	// Check if Peer collection exists, if not fail as Jalapeno topology is not running
	arango.peer, err = arango.db.Collection(context.TODO(), peer)
	if err != nil {
		return nil, err
	}

	//glog.Infof("create bgp_node collection")
	// create bgp_node collection
	var bgpNode_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.bgpNode, err = arango.db.CreateCollection(context.TODO(), "bgp_node", bgpNode_options)
	if err != nil {
		return nil, err
	}
	//glog.Infof("check bgp_node collection")

	// Check if eBGP Peer collection exists, if not fail as Jalapeno topology is not running
	arango.bgpNode, err = arango.db.Collection(context.TODO(), bgpNode)
	if err != nil {
		return nil, err
	}

	glog.Infof("create ebgp prefix v4 collection")
	// create ebgp prefix V4 collection
	var ebgpprefixV4_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.ebgpprefixV4, err = arango.db.CreateCollection(context.TODO(), "ebgp_prefix_v4", ebgpprefixV4_options)
	if err != nil {
		return nil, err
	}
	glog.Infof("check ebgp prefix v4 collection")

	// check if collection exists, if not fail as processor has failed to create collection
	arango.ebgpprefixV4, err = arango.db.Collection(context.TODO(), ebgpprefixV4)
	if err != nil {
		return nil, err
	}

	glog.Infof("create inet prefix v4 collection")
	// create inet prefix V4 collection
	var inetV4_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.inetprefixV4, err = arango.db.CreateCollection(context.TODO(), "inet_prefix_v4", inetV4_options)
	if err != nil {
		return nil, err
	}

	glog.Infof("check inet prefix v4 collection")
	// check if collection exists, if not fail as processor has failed to create collection
	arango.inetprefixV4, err = arango.db.Collection(context.TODO(), inetprefixV4)
	if err != nil {
		return nil, err
	}

	glog.Infof("create ibgp prefix v4 collection")
	// create ebgp prefix V4 collection
	var ibgpprefixV4_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.ibgpprefixV4, err = arango.db.CreateCollection(context.TODO(), "ibgp_prefix_v4", ibgpprefixV4_options)
	if err != nil {
		return nil, err
	}
	glog.Infof("check ibgp prefix v4 collection")

	// check if collection exists, if not fail as processor has failed to create collection
	arango.ibgpprefixV4, err = arango.db.Collection(context.TODO(), ibgpprefixV4)
	if err != nil {
		return nil, err
	}

	glog.Infof("checking for graph")
	// check for ipv4 topology graph
	found, err = arango.db.GraphExists(context.TODO(), ipv4Graph)
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
		edgeDefinition.From = []string{"bgp_node", "ebgp_prefix_v4", "inet_prefix_v4", "ibgp_prefix_v4"}
		edgeDefinition.To = []string{"bgp_node", "ebgp_prefix_v4", "inet_prefix_v4", "ibgp_prefix_v4"}
		var options driver.CreateGraphOptions
		options.EdgeDefinitions = []driver.EdgeDefinition{edgeDefinition}

		glog.Infof("creating graph %s", ipv4Graph)
		arango.ipv4Graph, err = arango.db.CreateGraph(context.TODO(), ipv4Graph, &options)
		if err != nil {
			return nil, err
		}
	}

	// check if graph exists, if not fail as processor has failed to create graph
	arango.ipv4Graph, err = arango.db.Graph(context.TODO(), ipv4Graph)
	glog.Infof("checking collection %s", ipv4Graph)
	if err != nil {
		return nil, err
	}

	// After creating/checking the graph, get the edge collection
	glog.Infof("getting graph edge collection")
	if arango.ipv4Graph != nil {
		// Get the edge collection from the graph
		arango.graph, err = arango.db.Collection(context.TODO(), "ipv4_graph")
		if err != nil {
			return nil, fmt.Errorf("failed to get graph edge collection: %v", err)
		}
		if arango.graph == nil {
			return nil, fmt.Errorf("graph edge collection is nil")
		}
	} else {
		return nil, fmt.Errorf("ipv4Graph is nil")
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
	case bmp.PeerStateChangeMsg:
		return a.peerHandler(event)
	}
	switch msgType {
	case bmp.UnicastPrefixV4Msg:
		return a.unicastprefixHandler(event)
	}
	return nil
}

// Start loading vertices and edges into the graph
func (a *arangoDB) loadEdge() error {
	ctx := context.TODO()
	glog.Infof("start processing vertices and edges")

	glog.Infof("insert link-state graph topology into ipv4 graph")
	copy_ls_topo := "for l in igpv4_graph insert l in ipv4_graph options { overwrite: " + "\"update\"" + " } "
	cursor, err := a.db.Query(ctx, copy_ls_topo, nil)
	if err != nil {
		glog.Errorf("Failed to copy link-state topology; it may not exist or have been populated in the database: %v", err)
	} else {
		defer cursor.Close()
	}

	glog.Infof("copying private ASN ebgp unicast v4 prefixes into ebgp_prefix_v4 collection")
	ebgp4_query := "FOR u IN unicast_prefix_v4 FILTER u.peer_asn IN 64512..65535 FILTER u.origin_as IN 64512..65535 " +
		"FILTER u.prefix_len < 30 FILTER u.base_attrs.as_path_count == 1 FOR p IN peer FILTER u.peer_ip == p.remote_ip " +
		"INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", u.prefix, u.prefix_len), prefix: u.prefix, prefix_len: u.prefix_len, " +
		"origin_as: u.origin_as, nexthop: u.nexthop, peer_ip: u.peer_ip, remote_ip: p.remote_ip, router_id: p.remote_bgp_id } " +
		"INTO ebgp_prefix_v4 OPTIONS { ignoreErrors: true } "
	cursor, err = a.db.Query(ctx, ebgp4_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	glog.Infof("copying public ASN unicast v4 prefixes into inet_prefix_v4 collection")
	inet4_query := "for u in unicast_prefix_v4 let internal_asns = ( for l in ls_node return l.peer_asn ) " +
		"filter u.peer_asn not in internal_asns filter u.peer_asn !in 64512..65535 filter u.origin_as !in 64512..65535 filter u.prefix_len < 96 " +
		"filter u.remote_asn != u.origin_as INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", u.prefix, u.prefix_len)," +
		"prefix: u.prefix, prefix_len: u.prefix_len, origin_as: u.origin_as, nexthop: u.nexthop } " +
		"INTO inet_prefix_v4 OPTIONS { ignoreErrors: true }"
	cursor, err = a.db.Query(ctx, inet4_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// iBGP goes last as we sort out which prefixes are orginated externally
	glog.Infof("copying ibgp unicast v4 prefixes into ibgp_prefix_v4 collection")
	ibgp4_query := "for u in unicast_prefix_v4 FILTER u.prefix_len < 30 filter u.base_attrs.local_pref != null " +
		"FILTER u.prefix_len < 30 FILTER u.base_attrs.as_path_count == null FOR p IN peer FILTER u.peer_ip == p.remote_ip " +
		"INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", u.prefix, u.prefix_len), prefix: u.prefix, prefix_len: u.prefix_len, " +
		"nexthop: u.nexthop, router_id: p.remote_bgp_id, asn: u.peer_asn, local_pref: u.base_attrs.local_pref } " +
		"INTO ibgp_prefix_v4 OPTIONS { ignoreErrors: true } "
	cursor, err = a.db.Query(ctx, ibgp4_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	glog.Infof("copying unique bgp_node into bgp_node collection")
	bgp_node_query := "for p in peer let igp_asns = ( for n in igp_node return n.peer_asn ) " +
		"filter p.remote_asn not in igp_asns " +
		"insert { _key: CONCAT_SEPARATOR(" + "\"_\", p.remote_bgp_id, p.remote_asn), " +
		"router_id: p.remote_bgp_id, asn: p.remote_asn  } INTO bgp_node OPTIONS { ignoreErrors: true }"
	cursor, err = a.db.Query(ctx, bgp_node_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// start building ipv4 graph
	peer2peer_query := "for p in peer return p"
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
		//glog.Infof("find ebgp peers to populate graph: %s", p.Key)
		if err := a.processPeerSession(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	bgp_prefix_query := "for p in ebgp_prefix_v4 return p"
	cursor, err = a.db.Query(ctx, bgp_prefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p bgpPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		//glog.Infof("get ipv eBGP prefixes: %s", p.Key)
		if err := a.processeBgpPrefix(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	inet_prefix_query := "for p in inet_prefix_v4 return p"
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
		//glog.Infof("get ipv inet prefixes: %s", p.Key)
		if err := a.processInetPrefix(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	ibgp_prefix_query := "for p in ibgp_prefix_v4 return p"
	cursor, err = a.db.Query(ctx, ibgp_prefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p ibgpPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		//glog.Infof("get ipv ibgp prefixes: %s", p.Key)
		if err := a.processIbgpPrefix(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	// Find eBGP egress / Inet peers from IGP domain. This could also be egress from IGP domain to internal eBGP peers
	bgp_query := "for l in peer let internal_asns = ( for n in igp_node return n.peer_asn ) " +
		"filter l.local_asn in internal_asns && l.remote_asn not in internal_asns filter l._key like " + "\"%:%\"" + " return l"
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

	return nil
}
