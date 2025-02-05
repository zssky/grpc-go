/*
 * Copyright 2019 gRPC authors.
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
	"encoding/json"
	"reflect"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/balancer/weightedroundrobin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/internal/xds/env"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/xds/pkg"
	"google.golang.org/grpc/xds/pkg/balancer/balancergroup"
	"google.golang.org/grpc/xds/pkg/balancer/weightedtarget/weightedaggregator"
	"google.golang.org/grpc/xds/pkg/client"
	xdsclient "google.golang.org/grpc/xds/pkg/client"
	"google.golang.org/grpc/xds/pkg/client/load"
)

// TODO: make this a environment variable?
var defaultPriorityInitTimeout = 10 * time.Second

const defaultServiceRequestCountMax = 1024

type localityConfig struct {
	weight uint32
	addrs  []resolver.Address
}

// balancerGroupWithConfig contains the localities with the same priority. It
// manages all localities using a balancerGroup.
type balancerGroupWithConfig struct {
	bg              *balancergroup.BalancerGroup
	stateAggregator *weightedaggregator.Aggregator
	configs         map[pkg.LocalityID]*localityConfig
}

// edsBalancerImpl does load balancing based on the EDS responses. Note that it
// doesn't implement the balancer interface. It's intended to be used by a high
// level balancer implementation.
//
// The localities are picked as weighted round robin. A configurable child
// policy is used to manage endpoints in each locality.
type edsBalancerImpl struct {
	cc           balancer.ClientConn
	buildOpts    balancer.BuildOptions
	logger       *grpclog.PrefixLogger
	loadReporter load.PerClusterReporter

	enqueueChildBalancerStateUpdate func(priorityType, balancer.State)

	subBalancerBuilder   balancer.Builder
	priorityToLocalities map[priorityType]*balancerGroupWithConfig
	respReceived         bool

	// There's no need to hold any mutexes at the same time. The order to take
	// mutex should be: priorityMu > subConnMu, but this is implicit via
	// balancers (starting balancer with next priority while holding priorityMu,
	// and the balancer may create new SubConn).

	priorityMu sync.Mutex
	// priorities are pointers, and will be nil when EDS returns empty result.
	priorityInUse   priorityType
	priorityLowest  priorityType
	priorityToState map[priorityType]*balancer.State
	// The timer to give a priority 10 seconds to connect. And if the priority
	// doesn't go into Ready/Failure, start the next priority.
	//
	// One timer is enough because there can be at most one priority in init
	// state.
	priorityInitTimer *time.Timer

	subConnMu         sync.Mutex
	subConnToPriority map[balancer.SubConn]priorityType

	pickerMu               sync.Mutex
	dropConfig             []xdsclient.OverloadDropConfig
	drops                  []*dropper
	innerState             balancer.State // The state of the picker without drop support.
	serviceRequestsCounter *client.ServiceRequestsCounter
	serviceRequestCountMax uint32
}

// newEDSBalancerImpl create a new edsBalancerImpl.
func newEDSBalancerImpl(cc balancer.ClientConn, bOpts balancer.BuildOptions, enqueueState func(priorityType, balancer.State), lr load.PerClusterReporter, logger *grpclog.PrefixLogger) *edsBalancerImpl {
	edsImpl := &edsBalancerImpl{
		cc:                 cc,
		buildOpts:          bOpts,
		logger:             logger,
		subBalancerBuilder: balancer.Get(roundrobin.Name),
		loadReporter:       lr,

		enqueueChildBalancerStateUpdate: enqueueState,

		priorityToLocalities:   make(map[priorityType]*balancerGroupWithConfig),
		priorityToState:        make(map[priorityType]*balancer.State),
		subConnToPriority:      make(map[balancer.SubConn]priorityType),
		serviceRequestCountMax: defaultServiceRequestCountMax,
	}
	// Don't start balancer group here. Start it when handling the first EDS
	// response. Otherwise the balancer group will be started with round-robin,
	// and if users specify a different sub-balancer, all balancers in balancer
	// group will be closed and recreated when sub-balancer update happens.
	return edsImpl
}

// handleChildPolicy updates the child balancers handling endpoints. Child
// policy is roundrobin by default. If the specified balancer is not installed,
// the old child balancer will be used.
//
// HandleChildPolicy and HandleEDSResponse must be called by the same goroutine.
func (edsImpl *edsBalancerImpl) handleChildPolicy(name string, config json.RawMessage) {
	if edsImpl.subBalancerBuilder.Name() == name {
		return
	}
	newSubBalancerBuilder := balancer.Get(name)
	if newSubBalancerBuilder == nil {
		edsImpl.logger.Infof("edsBalancerImpl: failed to find balancer with name %q, keep using %q", name, edsImpl.subBalancerBuilder.Name())
		return
	}
	edsImpl.subBalancerBuilder = newSubBalancerBuilder
	for _, bgwc := range edsImpl.priorityToLocalities {
		if bgwc == nil {
			continue
		}
		for lid, config := range bgwc.configs {
			lidJSON, err := lid.ToString()
			if err != nil {
				edsImpl.logger.Errorf("failed to marshal LocalityID: %#v, skipping this locality", lid)
				continue
			}
			// TODO: (eds) add support to balancer group to support smoothly
			//  switching sub-balancers (keep old balancer around until new
			//  balancer becomes ready).
			bgwc.bg.Remove(lidJSON)
			bgwc.bg.Add(lidJSON, edsImpl.subBalancerBuilder)
			bgwc.bg.UpdateClientConnState(lidJSON, balancer.ClientConnState{
				ResolverState: resolver.State{Addresses: config.addrs},
			})
			// This doesn't need to manually update picker, because the new
			// sub-balancer will send it's picker later.
		}
	}
}

// updateDrops compares new drop policies with the old. If they are different,
// it updates the drop policies and send ClientConn an updated picker.
func (edsImpl *edsBalancerImpl) updateDrops(dropConfig []xdsclient.OverloadDropConfig) {
	if cmp.Equal(dropConfig, edsImpl.dropConfig) {
		return
	}
	edsImpl.pickerMu.Lock()
	edsImpl.dropConfig = dropConfig
	var newDrops []*dropper
	for _, c := range edsImpl.dropConfig {
		newDrops = append(newDrops, newDropper(c))
	}
	edsImpl.drops = newDrops
	if edsImpl.innerState.Picker != nil {
		// Update picker with old inner picker, new drops.
		edsImpl.cc.UpdateState(balancer.State{
			ConnectivityState: edsImpl.innerState.ConnectivityState,
			Picker:            newDropPicker(edsImpl.innerState.Picker, newDrops, edsImpl.loadReporter, edsImpl.serviceRequestsCounter, edsImpl.serviceRequestCountMax)},
		)
	}
	edsImpl.pickerMu.Unlock()
}

// handleEDSResponse handles the EDS response and creates/deletes localities and
// SubConns. It also handles drops.
//
// HandleChildPolicy and HandleEDSResponse must be called by the same goroutine.
func (edsImpl *edsBalancerImpl) handleEDSResponse(edsResp xdsclient.EndpointsUpdate) {
	// TODO: Unhandled fields from EDS response:
	//  - edsResp.GetPolicy().GetOverprovisioningFactor()
	//  - locality.GetPriority()
	//  - lbEndpoint.GetMetadata(): contains BNS name, send to sub-balancers
	//    - as service config or as resolved address
	//  - if socketAddress is not ip:port
	//     - socketAddress.GetNamedPort(), socketAddress.GetResolverName()
	//     - resolve endpoint's name with another resolver

	// If the first EDS update is an empty update, nothing is changing from the
	// previous update (which is the default empty value). We need to explicitly
	// handle first update being empty, and send a transient failure picker.
	//
	// TODO: define Equal() on type EndpointUpdate to avoid DeepEqual. And do
	// the same for the other types.
	if !edsImpl.respReceived && reflect.DeepEqual(edsResp, xdsclient.EndpointsUpdate{}) {
		edsImpl.cc.UpdateState(balancer.State{ConnectivityState: connectivity.TransientFailure, Picker: base.NewErrPicker(errAllPrioritiesRemoved)})
	}
	edsImpl.respReceived = true

	edsImpl.updateDrops(edsResp.Drops)

	// Filter out all localities with weight 0.
	//
	// Locality weighted load balancer can be enabled by setting an option in
	// CDS, and the weight of each locality. Currently, without the guarantee
	// that CDS is always sent, we assume locality weighted load balance is
	// always enabled, and ignore all weight 0 localities.
	//
	// In the future, we should look at the config in CDS response and decide
	// whether locality weight matters.
	newLocalitiesWithPriority := make(map[priorityType][]xdsclient.Locality)
	for _, locality := range edsResp.Localities {
		if locality.Weight == 0 {
			continue
		}
		priority := newPriorityType(locality.Priority)
		newLocalitiesWithPriority[priority] = append(newLocalitiesWithPriority[priority], locality)
	}

	var (
		priorityLowest  priorityType
		priorityChanged bool
	)

	for priority, newLocalities := range newLocalitiesWithPriority {
		if !priorityLowest.isSet() || priorityLowest.higherThan(priority) {
			priorityLowest = priority
		}

		bgwc, ok := edsImpl.priorityToLocalities[priority]
		if !ok {
			// Create balancer group if it's never created (this is the first
			// time this priority is received). We don't start it here. It may
			// be started when necessary (e.g. when higher is down, or if it's a
			// new lowest priority).
			ccPriorityWrapper := edsImpl.ccWrapperWithPriority(priority)
			stateAggregator := weightedaggregator.New(ccPriorityWrapper, edsImpl.logger, newRandomWRR)
			bgwc = &balancerGroupWithConfig{
				bg:              balancergroup.New(ccPriorityWrapper, edsImpl.buildOpts, stateAggregator, edsImpl.loadReporter, edsImpl.logger),
				stateAggregator: stateAggregator,
				configs:         make(map[pkg.LocalityID]*localityConfig),
			}
			edsImpl.priorityToLocalities[priority] = bgwc
			priorityChanged = true
			edsImpl.logger.Infof("New priority %v added", priority)
		}
		edsImpl.handleEDSResponsePerPriority(bgwc, newLocalities)
	}
	edsImpl.priorityLowest = priorityLowest

	// Delete priorities that are removed in the latest response, and also close
	// the balancer group.
	for p, bgwc := range edsImpl.priorityToLocalities {
		if _, ok := newLocalitiesWithPriority[p]; !ok {
			delete(edsImpl.priorityToLocalities, p)
			bgwc.bg.Close()
			delete(edsImpl.priorityToState, p)
			priorityChanged = true
			edsImpl.logger.Infof("Priority %v deleted", p)
		}
	}

	// If priority was added/removed, it may affect the balancer group to use.
	// E.g. priorityInUse was removed, or all priorities are down, and a new
	// lower priority was added.
	if priorityChanged {
		edsImpl.handlePriorityChange()
	}
}

func (edsImpl *edsBalancerImpl) handleEDSResponsePerPriority(bgwc *balancerGroupWithConfig, newLocalities []xdsclient.Locality) {
	// newLocalitiesSet contains all names of localities in the new EDS response
	// for the same priority. It's used to delete localities that are removed in
	// the new EDS response.
	newLocalitiesSet := make(map[pkg.LocalityID]struct{})
	var rebuildStateAndPicker bool
	for _, locality := range newLocalities {
		// One balancer for each locality.

		lid := locality.ID
		lidJSON, err := lid.ToString()
		if err != nil {
			edsImpl.logger.Errorf("failed to marshal LocalityID: %#v, skipping this locality", lid)
			continue
		}
		newLocalitiesSet[lid] = struct{}{}

		newWeight := locality.Weight
		var newAddrs []resolver.Address
		for _, lbEndpoint := range locality.Endpoints {
			// Filter out all "unhealthy" endpoints (unknown and
			// healthy are both considered to be healthy:
			// https://www.envoyproxy.io/docs/envoy/latest/api-v2/api/v2/core/health_check.proto#envoy-api-enum-core-healthstatus).
			if lbEndpoint.HealthStatus != xdsclient.EndpointHealthStatusHealthy &&
				lbEndpoint.HealthStatus != xdsclient.EndpointHealthStatusUnknown {
				continue
			}

			address := resolver.Address{
				Addr: lbEndpoint.Address,
			}
			if edsImpl.subBalancerBuilder.Name() == weightedroundrobin.Name && lbEndpoint.Weight != 0 {
				ai := weightedroundrobin.AddrInfo{Weight: lbEndpoint.Weight}
				address = weightedroundrobin.SetAddrInfo(address, ai)
				// Metadata field in resolver.Address is deprecated. The
				// attributes field should be used to specify arbitrary
				// attributes about the address. We still need to populate the
				// Metadata field here to allow users of this field to migrate
				// to the new one.
				// TODO(easwars): Remove this once all users have migrated.
				// See https://github.com/grpc/grpc-go/issues/3563.
				address.Metadata = &ai
			}
			newAddrs = append(newAddrs, address)
		}
		var weightChanged, addrsChanged bool
		config, ok := bgwc.configs[lid]
		if !ok {
			// A new balancer, add it to balancer group and balancer map.
			bgwc.stateAggregator.Add(lidJSON, newWeight)
			bgwc.bg.Add(lidJSON, edsImpl.subBalancerBuilder)
			config = &localityConfig{
				weight: newWeight,
			}
			bgwc.configs[lid] = config

			// weightChanged is false for new locality, because there's no need
			// to update weight in bg.
			addrsChanged = true
			edsImpl.logger.Infof("New locality %v added", lid)
		} else {
			// Compare weight and addrs.
			if config.weight != newWeight {
				weightChanged = true
			}
			if !cmp.Equal(config.addrs, newAddrs) {
				addrsChanged = true
			}
			edsImpl.logger.Infof("Locality %v updated, weightedChanged: %v, addrsChanged: %v", lid, weightChanged, addrsChanged)
		}

		if weightChanged {
			config.weight = newWeight
			bgwc.stateAggregator.UpdateWeight(lidJSON, newWeight)
			rebuildStateAndPicker = true
		}

		if addrsChanged {
			config.addrs = newAddrs
			bgwc.bg.UpdateClientConnState(lidJSON, balancer.ClientConnState{
				ResolverState: resolver.State{Addresses: newAddrs},
			})
		}
	}

	// Delete localities that are removed in the latest response.
	for lid := range bgwc.configs {
		lidJSON, err := lid.ToString()
		if err != nil {
			edsImpl.logger.Errorf("failed to marshal LocalityID: %#v, skipping this locality", lid)
			continue
		}
		if _, ok := newLocalitiesSet[lid]; !ok {
			bgwc.stateAggregator.Remove(lidJSON)
			bgwc.bg.Remove(lidJSON)
			delete(bgwc.configs, lid)
			edsImpl.logger.Infof("Locality %v deleted", lid)
			rebuildStateAndPicker = true
		}
	}

	if rebuildStateAndPicker {
		bgwc.stateAggregator.BuildAndUpdate()
	}
}

// handleSubConnStateChange handles the state change and update pickers accordingly.
func (edsImpl *edsBalancerImpl) handleSubConnStateChange(sc balancer.SubConn, s connectivity.State) {
	edsImpl.subConnMu.Lock()
	var bgwc *balancerGroupWithConfig
	if p, ok := edsImpl.subConnToPriority[sc]; ok {
		if s == connectivity.Shutdown {
			// Only delete sc from the map when state changed to Shutdown.
			delete(edsImpl.subConnToPriority, sc)
		}
		bgwc = edsImpl.priorityToLocalities[p]
	}
	edsImpl.subConnMu.Unlock()
	if bgwc == nil {
		edsImpl.logger.Infof("edsBalancerImpl: priority not found for sc state change")
		return
	}
	if bg := bgwc.bg; bg != nil {
		bg.UpdateSubConnState(sc, balancer.SubConnState{ConnectivityState: s})
	}
}

// updateServiceRequestsConfig handles changes to the circuit breaking configuration.
func (edsImpl *edsBalancerImpl) updateServiceRequestsConfig(serviceName string, max *uint32) {
	if !env.CircuitBreakingSupport {
		return
	}
	edsImpl.pickerMu.Lock()
	var updatePicker bool
	if edsImpl.serviceRequestsCounter == nil || edsImpl.serviceRequestsCounter.ServiceName != serviceName {
		edsImpl.serviceRequestsCounter = client.GetServiceRequestsCounter(serviceName)
		updatePicker = true
	}

	var newMax uint32 = defaultServiceRequestCountMax
	if max != nil {
		newMax = *max
	}
	if edsImpl.serviceRequestCountMax != newMax {
		edsImpl.serviceRequestCountMax = newMax
		updatePicker = true
	}
	if updatePicker && edsImpl.innerState.Picker != nil {
		// Update picker with old inner picker, new counter and counterMax.
		edsImpl.cc.UpdateState(balancer.State{
			ConnectivityState: edsImpl.innerState.ConnectivityState,
			Picker:            newDropPicker(edsImpl.innerState.Picker, edsImpl.drops, edsImpl.loadReporter, edsImpl.serviceRequestsCounter, edsImpl.serviceRequestCountMax)},
		)
	}
	edsImpl.pickerMu.Unlock()
}

// updateState first handles priority, and then wraps picker in a drop picker
// before forwarding the update.
func (edsImpl *edsBalancerImpl) updateState(priority priorityType, s balancer.State) {
	_, ok := edsImpl.priorityToLocalities[priority]
	if !ok {
		edsImpl.logger.Infof("eds: received picker update from unknown priority")
		return
	}

	if edsImpl.handlePriorityWithNewState(priority, s) {
		edsImpl.pickerMu.Lock()
		defer edsImpl.pickerMu.Unlock()
		edsImpl.innerState = s
		// Don't reset drops when it's a state change.
		edsImpl.cc.UpdateState(balancer.State{ConnectivityState: s.ConnectivityState, Picker: newDropPicker(s.Picker, edsImpl.drops, edsImpl.loadReporter, edsImpl.serviceRequestsCounter, edsImpl.serviceRequestCountMax)})
	}
}

func (edsImpl *edsBalancerImpl) ccWrapperWithPriority(priority priorityType) *edsBalancerWrapperCC {
	return &edsBalancerWrapperCC{
		ClientConn: edsImpl.cc,
		priority:   priority,
		parent:     edsImpl,
	}
}

// edsBalancerWrapperCC implements the balancer.ClientConn API and get passed to
// each balancer group. It contains the locality priority.
type edsBalancerWrapperCC struct {
	balancer.ClientConn
	priority priorityType
	parent   *edsBalancerImpl
}

func (ebwcc *edsBalancerWrapperCC) NewSubConn(addrs []resolver.Address, opts balancer.NewSubConnOptions) (balancer.SubConn, error) {
	return ebwcc.parent.newSubConn(ebwcc.priority, addrs, opts)
}
func (ebwcc *edsBalancerWrapperCC) UpdateState(state balancer.State) {
	ebwcc.parent.enqueueChildBalancerStateUpdate(ebwcc.priority, state)
}

func (edsImpl *edsBalancerImpl) newSubConn(priority priorityType, addrs []resolver.Address, opts balancer.NewSubConnOptions) (balancer.SubConn, error) {
	sc, err := edsImpl.cc.NewSubConn(addrs, opts)
	if err != nil {
		return nil, err
	}
	edsImpl.subConnMu.Lock()
	edsImpl.subConnToPriority[sc] = priority
	edsImpl.subConnMu.Unlock()
	return sc, nil
}

// close closes the balancer.
func (edsImpl *edsBalancerImpl) close() {
	for _, bgwc := range edsImpl.priorityToLocalities {
		if bg := bgwc.bg; bg != nil {
			bgwc.stateAggregator.Stop()
			bg.Close()
		}
	}
}

type dropPicker struct {
	drops     []*dropper
	p         balancer.Picker
	loadStore load.PerClusterReporter
	counter   *client.ServiceRequestsCounter
	countMax  uint32
}

func newDropPicker(p balancer.Picker, drops []*dropper, loadStore load.PerClusterReporter, counter *client.ServiceRequestsCounter, countMax uint32) *dropPicker {
	return &dropPicker{
		drops:     drops,
		p:         p,
		loadStore: loadStore,
		counter:   counter,
		countMax:  countMax,
	}
}

func (d *dropPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	var (
		drop     bool
		category string
	)
	for _, dp := range d.drops {
		if dp.drop() {
			drop = true
			category = dp.c.Category
			break
		}
	}
	if drop {
		if d.loadStore != nil {
			d.loadStore.CallDropped(category)
		}
		return balancer.PickResult{}, status.Errorf(codes.Unavailable, "RPC is dropped")
	}
	if d.counter != nil {
		if err := d.counter.StartRequest(d.countMax); err != nil {
			// Drops by circuit breaking are reported with empty category. They
			// will be reported only in total drops, but not in per category.
			if d.loadStore != nil {
				d.loadStore.CallDropped("")
			}
			return balancer.PickResult{}, status.Errorf(codes.Unavailable, err.Error())
		}
		pr, err := d.p.Pick(info)
		if err != nil {
			d.counter.EndRequest()
			return pr, err
		}
		oldDone := pr.Done
		pr.Done = func(doneInfo balancer.DoneInfo) {
			d.counter.EndRequest()
			if oldDone != nil {
				oldDone(doneInfo)
			}
		}
		return pr, err
	}
	// TODO: (eds) don't drop unless the inner picker is READY. Similar to
	// https://github.com/grpc/grpc-go/issues/2622.
	return d.p.Pick(info)
}
