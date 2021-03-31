package cache

import (
	"errors"
	"fmt"
	"google.golang.org/grpc/xds/pkg/client"
	"net"
)

var (
	errResourceNotFound = errors.New("resource not found")
)

// UpdateCache - xds cache content
type UpdateCache struct {
	LDSVersion string
	LDSCache   map[string]client.ListenerUpdate
	RDSVersion string
	RDSCache   map[string]client.RouteConfigUpdate
	CDSVersion string
	CDSCache   map[string]client.ClusterUpdate
	EDSVersion string
	EDSCache   map[string]client.EndpointsUpdate
}

// FindListenerByAddress - find listener by address
func (u *UpdateCache) FindListenerByAddress(addr net.Addr) (*client.ListenerUpdate, error) {
	return nil, fmt.Errorf("method not implement")
}

// FindListenerByName - find listener by name
func (u *UpdateCache) FindListenerByName(name string) (*client.ListenerUpdate, error) {
	lis, ok := u.LDSCache[name]
	if !ok {
		return nil, errResourceNotFound
	}

	return &lis, nil
}

// FindRouteByName - find route by name
func (u *UpdateCache) FindRouteByName(name string) (*client.RouteConfigUpdate, error) {
	rt, ok := u.RDSCache[name]
	if !ok {
		return nil, errResourceNotFound
	}
	return &rt, nil
}

// FindClusterByName - find cluster by name
func (u *UpdateCache) FindClusterByName(name string) (*client.ClusterUpdate, error) {
	ct, ok := u.CDSCache[name]
	if !ok {
		return nil, errResourceNotFound
	}
	return &ct, nil
}

// FindEndpointsByName - find endpoint by name
func (u *UpdateCache) FindEndpointsByName(name string) (*client.EndpointsUpdate, error) {
	es, ok := u.EDSCache[name]
	if !ok {
		return nil, errResourceNotFound
	}
	return &es, nil
}

// FindEndpointsByListenerName - find endpoints by listener name
func (u *UpdateCache) FindEndpointsByListenerName(name string) (*client.EndpointsUpdate, error) {
	ls, ok := u.LDSCache[name]
	if !ok {
		return nil, errResourceNotFound
	}

	rt, ok := u.RDSCache[ls.RouteConfigName]
	if !ok {
		return nil, errResourceNotFound
	}

	// pick first
	var cluster string
	if len(rt.VirtualHosts) <= 0 && len(rt.VirtualHosts[0].Routes) <= 0 {
		return nil, errResourceNotFound
	}

	for key, _ := range rt.VirtualHosts[0].Routes[0].WeightedClusters {
		cluster = key
		break
	}

	cs, ok := u.CDSCache[cluster]
	if !ok {
		return nil, errResourceNotFound
	}

	// pick eds
	es, ok := u.EDSCache[cs.ServiceName]
	if !ok {
		return nil, errResourceNotFound
	}

	return &es, nil
}

