/*
 *
 * Copyright 2021 gRPC authors.
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
 *
 */

package client

func (c *clientImpl) cache(t ResourceType) (string, map[string]UpdateMetadata, interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var (
		version string
		md      map[string]UpdateMetadata
		cache   interface{}
	)
	switch t {
	case ListenerResource:
		version = c.ldsVersion
		md = c.ldsMD
		cache = c.ldsCache
	case RouteConfigResource:
		version = c.rdsVersion
		md = c.rdsMD
		cache = c.rdsCache
	case ClusterResource:
		version = c.cdsVersion
		md = c.cdsMD
		cache = c.cdsCache
	case EndpointsResource:
		version = c.edsVersion
		md = c.edsMD
		cache = c.edsCache
	default:
		c.logger.Errorf("dumping resource of unknown type: %v", t)
		return "", nil, nil
	}

	return version, md, cache
}

// LDSCache returns the status and contents of LDS.
func (c *clientImpl) LDSCache() (string, map[string]ListenerUpdate) {
	ver, _, inter := c.cache(ListenerResource)
	return ver, inter.(map[string]ListenerUpdate)
}

// RDSCache returns the status and contents of RDS.
func (c *clientImpl) RDSCache() (string, map[string]RouteConfigUpdate) {
	ver, _, inter := c.cache(RouteConfigResource)
	return ver, inter.(map[string]RouteConfigUpdate)
}

// CDSCache returns the status and contents of CDS.
func (c *clientImpl) CDSCache() (string, map[string]ClusterUpdate) {
	ver, _, inter := c.cache(ClusterResource)
	return ver, inter.(map[string]ClusterUpdate)
}

// EDSCache returns the status and contents of EDS.
func (c *clientImpl) EDSCache() (string, map[string]EndpointsUpdate) {
	ver, _, inter := c.cache(EndpointsResource)
	return ver, inter.(map[string]EndpointsUpdate)
}
