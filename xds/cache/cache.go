package cache

import (
	"fmt"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/xds/pkg/client"
	"google.golang.org/grpc/xds/pkg/client/bootstrap"

	_ "google.golang.org/grpc/xds/pkg/client/v2" // Register v2 xds_client.
	_ "google.golang.org/grpc/xds/pkg/client/v3" // Register v3 xds_client.
)

// xdsClientInterface contains methods from xdsClient.Client which are used by
// the server. This is useful for overriding in unit tests.
type xdsClientInterface interface {
	LDSCache() (string, map[string]client.ListenerUpdate)
	RDSCache() (string, map[string]client.RouteConfigUpdate)
	CDSCache() (string, map[string]client.ClusterUpdate)
	EDSCache() (string, map[string]client.EndpointsUpdate)
	BootstrapConfig() *bootstrap.Config
	Close()
}

var (
	logger       = grpclog.Component("xds")
	newXDSClient = func() (xdsClientInterface, error) {
		return client.New()
	}
)

// ClientConfigCache implementations client config cache.
type ClientConfigCache struct {
	// xdsClient will always be the same in practise. But we keep a copy in each
	// server instance for testing.
	xdsClient xdsClientInterface
}

// NewClientConfigCache returns an implementation of the client config cache
func NewClientConfigCache() (*ClientConfigCache, error) {
	xdsC, err := newXDSClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create xds client: %v", err)
	}
	return &ClientConfigCache{
		xdsClient: xdsC,
	}, nil
}

// FetchAll - fetch all client cache
func (s *ClientConfigCache) FetchAll() (*UpdateCache, error) {
	return s.buildClientCacheRespForReq()
}

// buildClientCacheRespForReq fetches the status from the client, and returns
// the response to be sent back to client.
//
// If it returns an error, the error is a status error.
func (s *ClientConfigCache) buildClientCacheRespForReq() (*UpdateCache, error) {
	var ret UpdateCache

	ret.LDSVersion, ret.LDSCache = s.buildLDSCache()
	ret.RDSVersion, ret.RDSCache = s.buildRDSCache()
	ret.CDSVersion, ret.CDSCache = s.buildCDSCache()
	ret.EDSVersion, ret.EDSCache = s.buildEDSCache()
	return &ret, nil
}

// Close cleans up the resources.
func (s *ClientConfigCache) Close() {
	s.xdsClient.Close()
}

func (s *ClientConfigCache) buildLDSCache() (string, map[string]client.ListenerUpdate) {
	return s.xdsClient.LDSCache()
}

func (s *ClientConfigCache) buildRDSCache() (string, map[string]client.RouteConfigUpdate) {
	return s.xdsClient.RDSCache()
}

func (s *ClientConfigCache) buildCDSCache() (string, map[string]client.ClusterUpdate) {
	return s.xdsClient.CDSCache()
}

func (s *ClientConfigCache) buildEDSCache() (string, map[string]client.EndpointsUpdate) {
	return s.xdsClient.EDSCache()
}
