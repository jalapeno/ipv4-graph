package arangodb

import (
	"context"
	"strconv"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) processInetPrefix(ctx context.Context, key string, e *message.UnicastPrefix) error {
	if e.PeerASN >= 64512 && e.PeerASN <= 65535 {
		return a.processeNewPrefix(ctx, key, e)
	} else {
		query := "for l in bgp_node filter l.asn !in 64512..65535"
		query += " return l"

		pcursor, err := a.db.Query(ctx, query, nil)
		if err != nil {
			return err
		}
		defer pcursor.Close()
		for {
			var up inetPrefix
			mp, err := pcursor.ReadDocument(ctx, &up)
			if err != nil {
				if driver.IsNoMoreDocuments(err) {
					return err
				}
				if !driver.IsNoMoreDocuments(err) {
					return err
				}
				break
			}

			glog.Infof("ebgp peer %s + eBGP prefix %s, meta %+v", e.Key, up.Key, mp)
			from := unicastPrefixEdgeObject{
				Key:       up.Key + "_" + e.Key,
				From:      up.ID,
				To:        e.ID,
				Prefix:    up.Prefix,
				PrefixLen: up.PrefixLen,
				OriginAS:  up.OriginAS,
			}

			if _, err := a.graph.CreateDocument(ctx, &from); err != nil {
				if !driver.IsConflict(err) {
					return err
				}
				// The document already exists, updating it with the latest info
				if _, err := a.graph.UpdateDocument(ctx, from.Key, &from); err != nil {
					return err
				}
			}
			to := unicastPrefixEdgeObject{
				Key:       e.Key + "_" + up.Key,
				From:      e.ID,
				To:        up.ID,
				Prefix:    up.Prefix,
				PrefixLen: up.PrefixLen,
				OriginAS:  up.OriginAS,
			}

			if _, err := a.graph.CreateDocument(ctx, &to); err != nil {
				if !driver.IsConflict(err) {
					return err
				}
				// The document already exists, updating it with the latest info
				if _, err := a.graph.UpdateDocument(ctx, to.Key, &to); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (a *arangoDB) processeBgpPrefix(ctx context.Context, key string, e *bgpPrefix) error {
	// get internal ASN so we can determine whether this is an external prefix or not

	// if e.BaseAttributes.ASPathCount != 1 {
	// 	return nil
	// }
	query := "for l in bgp_node filter l.router_id == '" + e.RouterID + "' and l.asn == " + strconv.Itoa(int(e.OriginAS))
	query += " return l	"
	glog.Infof("query: %+v", query)
	pcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()
	for {
		var up inetPrefix
		mp, err := pcursor.ReadDocument(ctx, &up)
		if err != nil {
			if driver.IsNoMoreDocuments(err) {
				return err
			}
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}

		glog.Infof("eBGP node and unicastprefix %s, meta %+v, %v", e.Key, up.Key, mp)
		from := unicastPrefixEdgeObject{
			Key:       up.Key + "_" + e.Key,
			From:      up.ID,
			To:        e.ID,
			Prefix:    up.Prefix,
			PrefixLen: up.PrefixLen,
			OriginAS:  up.OriginAS,
		}

		if _, err := a.graph.CreateDocument(ctx, &from); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.graph.UpdateDocument(ctx, from.Key, &from); err != nil {
				return err
			}
		}
		to := unicastPrefixEdgeObject{
			Key:       e.Key + "_" + up.Key,
			From:      e.ID,
			To:        up.ID,
			Prefix:    up.Prefix,
			PrefixLen: up.PrefixLen,
			OriginAS:  up.OriginAS,
		}

		if _, err := a.graph.CreateDocument(ctx, &to); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.graph.UpdateDocument(ctx, to.Key, &to); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *arangoDB) processeIbgpPrefix(ctx context.Context, key string, e *ibgpPrefix) error {
	query := "for l in igp_node filter l.router_id == '" + e.RouterID + "' and l.asn == " + strconv.Itoa(int(e.ASN))
	query += " return l	"
	glog.Infof("query: %+v", query)
	pcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()
	for {
		var up ibgpPrefix
		mp, err := pcursor.ReadDocument(ctx, &up)
		if err != nil {
			if driver.IsNoMoreDocuments(err) {
				return err
			}
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}

		glog.Infof("ibgp node and unicastprefix %s, meta %+v, %v", e.Key, up.Key, mp)
		from := unicastPrefixEdgeObject{
			Key:       up.Key + "_" + e.Key,
			From:      up.ID,
			To:        e.ID,
			Prefix:    up.Prefix,
			PrefixLen: up.PrefixLen,
			ASN:       e.ASN,
		}

		if _, err := a.graph.CreateDocument(ctx, &from); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.graph.UpdateDocument(ctx, from.Key, &from); err != nil {
				return err
			}
		}
		to := unicastPrefixEdgeObject{
			Key:       e.Key + "_" + up.Key,
			From:      e.ID,
			To:        up.ID,
			Prefix:    up.Prefix,
			PrefixLen: up.PrefixLen,
			ASN:       e.ASN,
		}

		if _, err := a.graph.CreateDocument(ctx, &to); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.graph.UpdateDocument(ctx, to.Key, &to); err != nil {
				return err
			}
		}
	}
	return nil
}

// processeNewPrefix is under construction
func (a *arangoDB) processeNewPrefix(ctx context.Context, key string, e *message.UnicastPrefix) error {
	// get internal ASN so we can determine whether this is an external prefix or not

	// if e.BaseAttributes.ASPathCount != 1 {
	// 	return nil
	// }
	query := "for l in bgp_node filter l.router_id == '" + e.PeerIP + "' and l.asn == " + strconv.Itoa(int(e.OriginAS))
	query += " return l	"
	glog.Infof("query: %+v", query)
	pcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()
	for {
		var up inetPrefix
		mp, err := pcursor.ReadDocument(ctx, &up)
		if err != nil {
			if driver.IsNoMoreDocuments(err) {
				return err
			}
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}

		glog.Infof("eBGP node and unicastprefix %s, meta %+v, %v", e.Key, up.Key, mp)
		from := unicastPrefixEdgeObject{
			Key:       up.Key + "_" + e.Key,
			From:      up.ID,
			To:        e.ID,
			Prefix:    up.Prefix,
			PrefixLen: up.PrefixLen,
			OriginAS:  up.OriginAS,
		}

		if _, err := a.graph.CreateDocument(ctx, &from); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.graph.UpdateDocument(ctx, from.Key, &from); err != nil {
				return err
			}
		}
		to := unicastPrefixEdgeObject{
			Key:       e.Key + "_" + up.Key,
			From:      e.ID,
			To:        up.ID,
			Prefix:    up.Prefix,
			PrefixLen: up.PrefixLen,
			OriginAS:  up.OriginAS,
		}

		if _, err := a.graph.CreateDocument(ctx, &to); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.graph.UpdateDocument(ctx, to.Key, &to); err != nil {
				return err
			}
		}
	}
	return nil
}

// processEdgeRemoval removes a record from Node's graph collection
// since the key matches in both collections (LS Links and Nodes' Graph) deleting the record directly.
func (a *arangoDB) processUnicastPrefixRemoval(ctx context.Context, key string) error {
	if _, err := a.graph.RemoveDocument(ctx, key); err != nil {
		if !driver.IsNotFound(err) {
			return err
		}
		return nil
	}
	return nil
}
