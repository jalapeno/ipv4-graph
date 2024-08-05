package arangodb

import (
	"context"
	"fmt"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

// processEdge processes a single ls_link connection which is a unidirectional edge between two nodes (vertices).
func (a *arangoDB) processASBR(ctx context.Context, key string, l *message.PeerStateChange) error {
	glog.Infof("processing ASBRs for peer session  : %s", l.ID)
	// get local asbr / ls_node_extended from peer entry
	ln, err := a.getLSNodeExt(ctx, l, true)
	if err != nil {
		glog.Errorf("processASBR failed to get local lsnode %s for link: %s with error: %+v", l.LocalBGPID, l.ID, err)
		return err
	}

	// get remote node from ls_link entry
	rn, err := a.getLSNodeExt(ctx, l, false)
	if err != nil {
		glog.Errorf("processASBR failed to get remote lsnode %s for link: %s with error: %+v", l.RemoteBGPID, l.ID, err)
		return err
	}
	glog.V(6).Infof("Local node -> Protocol: %+v Domain ID: %+v IGP Router ID: %+v", ln.ProtocolID, ln.DomainID, ln.IGPRouterID)
	glog.V(6).Infof("Remote node -> Protocol: %+v Domain ID: %+v IGP Router ID: %+v", rn.ProtocolID, rn.DomainID, rn.IGPRouterID)
	if err := a.createAsbrEdgeObject(ctx, l, ln, rn); err != nil {
		glog.Errorf("processASBR failed to create Edge object with error: %+v", err)
		glog.Errorf("Local node -> Protocol: %+v Domain ID: %+v IGP Router ID: %+v", ln.ProtocolID, ln.DomainID, ln.IGPRouterID)
		glog.Errorf("Remote node -> Protocol: %+v Domain ID: %+v IGP Router ID: %+v", rn.ProtocolID, rn.DomainID, rn.IGPRouterID)
		return err
	}
	//glog.V(9).Infof("processEdge completed processing lslink: %s for ls nodes: %s - %s", l.ID, ln.ID, rn.ID)

	return nil
}

// processLinkRemoval removes a record from Node's graph collection
// since the key matches in both collections (LS Links and Nodes' Graph) deleting the record directly.
func (a *arangoDB) processASBRRemoval(ctx context.Context, key string, action string) error {
	if _, err := a.graph.RemoveDocument(ctx, key); err != nil {
		if !driver.IsNotFound(err) {
			return err
		}
		return nil
	}

	return nil
}

func (a *arangoDB) getLSNodeExt(ctx context.Context, e *message.PeerStateChange, local bool) (*LSNodeExt, error) {
	// Need to find ls_node object matching ls_link's IGP Router ID
	query := "FOR d IN " + a.lsnodeExt.Name()
	if local {
		//glog.Infof("getNode local node per link: %s, %s, %v", e.IGPRouterID, e.ID, e.ProtocolID)
		query += " filter d.router_id == " + "\"" + e.LocalBGPID + "\""
	} else {
		//glog.Infof("getNode remote node per link: %s, %s, %v", e.RemoteIGPRouterID, e.ID, e.ProtocolID)
		query += " filter d.router_id == " + "\"" + e.RemoteBGPID + "\""
	}
	query += " return d"
	//glog.Infof("query: %s", query)
	lcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return nil, err
	}
	defer lcursor.Close()
	var ln LSNodeExt
	i := 0
	for ; ; i++ {
		_, err := lcursor.ReadDocument(ctx, &ln)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return nil, err
			}
			break
		}
	}
	if i == 0 {
		return nil, fmt.Errorf("query %s returned 0 results", query)
	}
	if i > 1 {
		return nil, fmt.Errorf("query %s returned more than 1 result", query)
	}

	return &ln, nil
}

func (a *arangoDB) createAsbrEdgeObject(ctx context.Context, l *message.PeerStateChange, ln, rn *LSNodeExt) error {

	ne := peerToObject{
		Key:       l.Key,
		From:      ln.ID,
		To:        rn.ID,
		LocalIP:   l.LocalIP,
		RemoteIP:  l.RemoteIP,
		LocalASN:  l.LocalASN,
		RemoteASN: l.RemoteASN,
	}
	if _, err := a.graph.CreateDocument(ctx, &ne); err != nil {
		if !driver.IsConflict(err) {
			return err
		}
		// The document already exists, updating it with the latest info
		if _, err := a.graph.UpdateDocument(ctx, ne.Key, &ne); err != nil {
			return err
		}
	}

	return nil
}
