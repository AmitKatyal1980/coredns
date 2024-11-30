package loadaware

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/coredns/coredns/plugin"
	"github.com/redis/go-redis/v9"

	"github.com/miekg/dns"
)

type LoadAware struct {
	Next          plugin.Handler
	RedisClient   *redis.Client
	RedisKey      string
	LoadThreshold int
}

type ConnStore struct {
	Address string `json:"address"`
	Count   int    `json:"count"`
}

type LoadAwareResponseWriter struct {
	dns.ResponseWriter
	LC *LoadAware
}

func (lw *LoadAwareResponseWriter) WriteMsg(res *dns.Msg) error {
	if res == nil {
		return lw.ResponseWriter.WriteMsg(res)
	}

	log.Info("response writer invoked")

	ipMap := make(map[string]*dns.A)
	var otherRecords []dns.RR

	// Extract A records and other records
	for _, answer := range res.Answer {
		if aRecord, ok := answer.(*dns.A); ok {
			ipMap[aRecord.A.String()] = aRecord
		} else {
			otherRecords = append(otherRecords, answer)
		}
	}

	log.Infof("IP map: %v", ipMap)

	// Sort IPs by least connection
	sortedIPs, err := lw.LC.getSortedHostsByLoad(ipMap)
	if err != nil {
		return lw.ResponseWriter.WriteMsg(res)
	}

	// Reorder the response
	var newAnswers []dns.RR
	newAnswers = append(newAnswers, otherRecords...)
	for _, ip := range sortedIPs {
		newAnswers = append(newAnswers, ipMap[ip])
	}

	res.Answer = newAnswers
	return lw.ResponseWriter.WriteMsg(res)
}

func (lc *LoadAware) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	lw := &LoadAwareResponseWriter{
		ResponseWriter: w,
		LC:             lc,
	}
	// pass the request to the next plugin in the chain
	return plugin.NextOrFailure(lc.Name(), lc.Next, ctx, lw, r)
}

func (lc *LoadAware) getSortedHostsByLoad(ipMap map[string]*dns.A) ([]string, error) {
	ipConnectionCount := make(map[string]int)

	// Query Redis for the connection counts for each IP
	for ip := range ipMap {
		connCount, err := lc.getConnectionCountForIP(ip)
		log.Infof("ip address : %v count :%v", ip, connCount)
		if err != nil {
			return nil, err
		}

		if connCount < lc.LoadThreshold {
			ipConnectionCount[ip] = connCount
		}
	}

	// if all the instances are fully loaded
	if len(ipConnectionCount) == 0 {
		var sortedIPs []string
		for ip := range ipMap {
			sortedIPs = append(sortedIPs, ip)
		}
		return sortedIPs, nil
	}

	// Sort the IPs by connection count
	var sortedIPs []string
	for ip := range ipConnectionCount {
		sortedIPs = append(sortedIPs, ip)
	}
	sort.Slice(sortedIPs, func(i, j int) bool {
		return ipConnectionCount[sortedIPs[i]] < ipConnectionCount[sortedIPs[j]]
	})
	log.Infof("sorted connection count : %v", sortedIPs)
	return sortedIPs, nil
}

func (lc *LoadAware) getConnectionCountForIP(ip string) (int, error) {
	log.Infof("redis key: %s", lc.RedisKey)

	connData, err := lc.RedisClient.HGet(context.Background(), lc.RedisKey, ip).Result()

	if err != nil {
		if err == redis.Nil {
			// No data for this IP
			log.Warningf("No data found for IP: %s", ip)
			return 0, nil
		}
		log.Errorf("err: %v", err)
		return 0, err
	}

	log.Infof("connData: %v", connData)
	// Parse JSON into ConnStore
	var connStore ConnStore
	err = json.Unmarshal([]byte(connData), &connStore)
	if err != nil {
		log.Errorf("Error parsing JSON for IP %s: %v", ip, err)
		return 0, err
	}
	return connStore.Count, nil
}

func (lc *LoadAware) Name() string { return loadAware }
