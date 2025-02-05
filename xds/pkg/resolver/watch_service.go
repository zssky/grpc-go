/*
 *
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
 *
 */

package resolver

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/internal/grpclog"
	xdsclient "google.golang.org/grpc/xds/pkg/client"
)

// serviceUpdate contains information received from the LDS/RDS responses which
// are of interest to the xds resolver. The RDS request is built by first
// making a LDS to get the RouteConfig name.
type serviceUpdate struct {
	// virtualHost contains routes and other configuration to route RPCs.
	virtualHost *xdsclient.VirtualHost
	// ldsConfig contains configuration that applies to all routes.
	ldsConfig ldsConfig
}

// ldsConfig contains information received from the LDS responses which are of
// interest to the xds resolver.
type ldsConfig struct {
	// maxStreamDuration is from the HTTP connection manager's
	// common_http_protocol_options field.
	maxStreamDuration time.Duration
	httpFilterConfig  []xdsclient.HTTPFilter
}

// watchService uses LDS and RDS to discover information about the provided
// serviceName.
//
// Note that during race (e.g. an xDS response is received while the user is
// calling cancel()), there's a small window where the callback can be called
// after the watcher is canceled. The caller needs to handle this case.
func watchService(c xdsClientInterface, serviceName string, cb func(serviceUpdate, error), logger *grpclog.PrefixLogger) (cancel func()) {
	w := &serviceUpdateWatcher{
		logger:      logger,
		c:           c,
		serviceName: serviceName,
		serviceCb:   cb,
	}
	w.ldsCancel = c.WatchListener(serviceName, w.handleLDSResp)

	return w.close
}

// serviceUpdateWatcher handles LDS and RDS response, and calls the service
// callback at the right time.
type serviceUpdateWatcher struct {
	logger      *grpclog.PrefixLogger
	c           xdsClientInterface
	serviceName string
	ldsCancel   func()
	serviceCb   func(serviceUpdate, error)
	lastUpdate  serviceUpdate

	mu        sync.Mutex
	closed    bool
	rdsName   string
	rdsCancel func()
}

func (w *serviceUpdateWatcher) handleLDSResp(update xdsclient.ListenerUpdate, err error) {
	w.logger.Infof("received LDS update: %+v, err: %v", update, err)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	if err != nil {
		// We check the error type and do different things. For now, the only
		// type we check is ResourceNotFound, which indicates the LDS resource
		// was removed, and besides sending the error to callback, we also
		// cancel the RDS watch.
		if xdsclient.ErrType(err) == xdsclient.ErrorTypeResourceNotFound && w.rdsCancel != nil {
			w.rdsCancel()
			w.rdsName = ""
			w.rdsCancel = nil
			w.lastUpdate = serviceUpdate{}
		}
		// The other error cases still return early without canceling the
		// existing RDS watch.
		w.serviceCb(serviceUpdate{}, err)
		return
	}

	w.lastUpdate.ldsConfig = ldsConfig{
		maxStreamDuration: update.MaxStreamDuration,
		httpFilterConfig:  update.HTTPFilters,
	}

	if w.rdsName == update.RouteConfigName {
		// If the new RouteConfigName is same as the previous, don't cancel and
		// restart the RDS watch.
		//
		// If the route name did change, then we must wait until the first RDS
		// update before reporting this LDS config.
		w.serviceCb(w.lastUpdate, nil)
		return
	}
	w.rdsName = update.RouteConfigName
	if w.rdsCancel != nil {
		w.rdsCancel()
	}
	w.rdsCancel = w.c.WatchRouteConfig(update.RouteConfigName, w.handleRDSResp)
}

func (w *serviceUpdateWatcher) handleRDSResp(update xdsclient.RouteConfigUpdate, err error) {
	w.logger.Infof("received RDS update: %+v, err: %v", update, err)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	if w.rdsCancel == nil {
		// This mean only the RDS watch is canceled, can happen if the LDS
		// resource is removed.
		return
	}
	if err != nil {
		w.serviceCb(serviceUpdate{}, err)
		return
	}

	matchVh := findBestMatchingVirtualHost(w.serviceName, update.VirtualHosts)
	if matchVh == nil {
		// No matching virtual host found.
		w.serviceCb(serviceUpdate{}, fmt.Errorf("no matching virtual host found for %q", w.serviceName))
		return
	}

	w.lastUpdate.virtualHost = matchVh
	w.serviceCb(w.lastUpdate, nil)
}

func (w *serviceUpdateWatcher) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	w.ldsCancel()
	if w.rdsCancel != nil {
		w.rdsCancel()
		w.rdsCancel = nil
	}
}

type domainMatchType int

const (
	domainMatchTypeInvalid domainMatchType = iota
	domainMatchTypeUniversal
	domainMatchTypePrefix
	domainMatchTypeSuffix
	domainMatchTypeExact
)

// Exact > Suffix > Prefix > Universal > Invalid.
func (t domainMatchType) betterThan(b domainMatchType) bool {
	return t > b
}

func matchTypeForDomain(d string) domainMatchType {
	if d == "" {
		return domainMatchTypeInvalid
	}
	if d == "*" {
		return domainMatchTypeUniversal
	}
	if strings.HasPrefix(d, "*") {
		return domainMatchTypeSuffix
	}
	if strings.HasSuffix(d, "*") {
		return domainMatchTypePrefix
	}
	if strings.Contains(d, "*") {
		return domainMatchTypeInvalid
	}
	return domainMatchTypeExact
}

func match(domain, host string) (domainMatchType, bool) {
	switch typ := matchTypeForDomain(domain); typ {
	case domainMatchTypeInvalid:
		return typ, false
	case domainMatchTypeUniversal:
		return typ, true
	case domainMatchTypePrefix:
		// abc.*
		return typ, strings.HasPrefix(host, strings.TrimSuffix(domain, "*"))
	case domainMatchTypeSuffix:
		// *.123
		return typ, strings.HasSuffix(host, strings.TrimPrefix(domain, "*"))
	case domainMatchTypeExact:
		return typ, domain == host
	default:
		return domainMatchTypeInvalid, false
	}
}

// findBestMatchingVirtualHost returns the virtual host whose domains field best
// matches host
//
// The domains field support 4 different matching pattern types:
//  - Exact match
//  - Suffix match (e.g. “*ABC”)
//  - Prefix match (e.g. “ABC*)
//  - Universal match (e.g. “*”)
//
// The best match is defined as:
//  - A match is better if it’s matching pattern type is better
//    - Exact match > suffix match > prefix match > universal match
//  - If two matches are of the same pattern type, the longer match is better
//    - This is to compare the length of the matching pattern, e.g. “*ABCDE” >
//    “*ABC”
func findBestMatchingVirtualHost(host string, vHosts []*xdsclient.VirtualHost) *xdsclient.VirtualHost {
	var (
		matchVh   *xdsclient.VirtualHost
		matchType = domainMatchTypeInvalid
		matchLen  int
	)
	for _, vh := range vHosts {
		for _, domain := range vh.Domains {
			typ, matched := match(domain, host)
			if typ == domainMatchTypeInvalid {
				// The rds response is invalid.
				return nil
			}
			if matchType.betterThan(typ) || matchType == typ && matchLen >= len(domain) || !matched {
				// The previous match has better type, or the previous match has
				// better length, or this domain isn't a match.
				continue
			}
			matchVh = vh
			matchType = typ
			matchLen = len(domain)
		}
	}
	return matchVh
}
