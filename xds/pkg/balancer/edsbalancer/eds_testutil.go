/*
 * Copyright 2020 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edsbalancer

import (
	"fmt"
	"net"
	"strconv"

	xdspb "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	corepb "github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	endpointpb "github.com/envoyproxy/go-control-plane/envoy/api/v2/endpoint"
	typepb "github.com/envoyproxy/go-control-plane/envoy/type"
	"google.golang.org/grpc/xds/pkg"
	xdsclient "google.golang.org/grpc/xds/pkg/client"
)

// parseEDSRespProtoForTesting parses EDS response, and panic if parsing fails.
//
// TODO: delete this. The EDS balancer tests should build an EndpointsUpdate
// directly, instead of building and parsing a proto message.
func parseEDSRespProtoForTesting(m *xdspb.ClusterLoadAssignment) xdsclient.EndpointsUpdate {
	u, err := parseEDSRespProto(m)
	if err != nil {
		panic(err.Error())
	}
	return u
}

// parseEDSRespProto turns EDS response proto message to EndpointsUpdate.
func parseEDSRespProto(m *xdspb.ClusterLoadAssignment) (xdsclient.EndpointsUpdate, error) {
	ret := xdsclient.EndpointsUpdate{}
	for _, dropPolicy := range m.GetPolicy().GetDropOverloads() {
		ret.Drops = append(ret.Drops, parseDropPolicy(dropPolicy))
	}
	priorities := make(map[uint32]struct{})
	for _, locality := range m.Endpoints {
		l := locality.GetLocality()
		if l == nil {
			return xdsclient.EndpointsUpdate{}, fmt.Errorf("EDS response contains a locality without ID, locality: %+v", locality)
		}
		lid := pkg.LocalityID{
			Region:  l.Region,
			Zone:    l.Zone,
			SubZone: l.SubZone,
		}
		priority := locality.GetPriority()
		priorities[priority] = struct{}{}
		ret.Localities = append(ret.Localities, xdsclient.Locality{
			ID:        lid,
			Endpoints: parseEndpoints(locality.GetLbEndpoints()),
			Weight:    locality.GetLoadBalancingWeight().GetValue(),
			Priority:  priority,
		})
	}
	for i := 0; i < len(priorities); i++ {
		if _, ok := priorities[uint32(i)]; !ok {
			return xdsclient.EndpointsUpdate{}, fmt.Errorf("priority %v missing (with different priorities %v received)", i, priorities)
		}
	}
	return ret, nil
}

func parseAddress(socketAddress *corepb.SocketAddress) string {
	return net.JoinHostPort(socketAddress.GetAddress(), strconv.Itoa(int(socketAddress.GetPortValue())))
}

func parseDropPolicy(dropPolicy *xdspb.ClusterLoadAssignment_Policy_DropOverload) xdsclient.OverloadDropConfig {
	percentage := dropPolicy.GetDropPercentage()
	var (
		numerator   = percentage.GetNumerator()
		denominator uint32
	)
	switch percentage.GetDenominator() {
	case typepb.FractionalPercent_HUNDRED:
		denominator = 100
	case typepb.FractionalPercent_TEN_THOUSAND:
		denominator = 10000
	case typepb.FractionalPercent_MILLION:
		denominator = 1000000
	}
	return xdsclient.OverloadDropConfig{
		Category:    dropPolicy.GetCategory(),
		Numerator:   numerator,
		Denominator: denominator,
	}
}

func parseEndpoints(lbEndpoints []*endpointpb.LbEndpoint) []xdsclient.Endpoint {
	endpoints := make([]xdsclient.Endpoint, 0, len(lbEndpoints))
	for _, lbEndpoint := range lbEndpoints {
		endpoints = append(endpoints, xdsclient.Endpoint{
			HealthStatus: xdsclient.EndpointHealthStatus(lbEndpoint.GetHealthStatus()),
			Address:      parseAddress(lbEndpoint.GetEndpoint().GetAddress().GetSocketAddress()),
			Weight:       lbEndpoint.GetLoadBalancingWeight().GetValue(),
		})
	}
	return endpoints
}
