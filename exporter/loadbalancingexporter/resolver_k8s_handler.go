// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

var _ cache.ResourceEventHandler = (*handler)(nil)

const (
	epMissingHostnamesMsg = "EndpointSlice object missing hostnames"
)

type handler struct {
	endpoints   *sync.Map
	callback    func(ctx context.Context) ([]string, error)
	logger      *zap.Logger
	telemetry   *metadata.TelemetryBuilder
	returnNames bool
	// hostIPs keeps track of current IP for a hostname when returnNames is true so we can detect IP changes.
	hostIPs *sync.Map
}

func (h handler) OnAdd(obj any, _ bool) {
	var endpoints map[string]string
	var ok bool

	switch object := obj.(type) {
	case *discoveryv1.EndpointSlice:
		ok, endpoints = convertToEndpoints(h.returnNames, object)
		if !ok {
			h.logger.Warn(epMissingHostnamesMsg, zap.Any("obj", obj))
			h.telemetry.LoadbalancerNumResolutions.Add(context.Background(), 1, metric.WithAttributeSet(k8sResolverFailureAttrSet))
			return
		}
	default:
		h.logger.Warn("Got an unexpected Kubernetes data type during inclusion", zap.Any("obj", obj))
		h.telemetry.LoadbalancerNumResolutions.Add(context.Background(), 1, metric.WithAttributeSet(k8sResolverFailureAttrSet))
		return
	}
	// Track whether we saw at least one brand new endpoint.
	changed := false
	for ep, ip := range endpoints {
		if _, present := h.endpoints.Load(ep); !present {
			h.endpoints.Store(ep, true)
			changed = true
		}
		if h.returnNames && h.hostIPs != nil {
			h.hostIPs.Store(ep, ip)
		}
	}
	if changed {
		_, _ = h.callback(context.Background())
	} else {
		h.logger.Debug("OnAdd received slice but no new endpoints detected")
	}
}

func (h handler) OnUpdate(oldObj, newObj any) {
	switch oldSlice := oldObj.(type) {
	case *discoveryv1.EndpointSlice:
		newSlice, ok := newObj.(*discoveryv1.EndpointSlice)
		if !ok {
			h.logger.Warn("Unexpected Kubernetes data type during update", zap.Any("obj", newObj))
			h.telemetry.LoadbalancerNumResolutions.Add(context.Background(), 1, metric.WithAttributeSet(k8sResolverFailureAttrSet))
			return
		}
		_, oldEndpoints := convertToEndpoints(h.returnNames, oldSlice)
		hostnameOk, newEndpoints := convertToEndpoints(h.returnNames, newSlice)
		if !hostnameOk {
			h.logger.Warn(epMissingHostnamesMsg, zap.Any("obj", newSlice))
			h.telemetry.LoadbalancerNumResolutions.Add(context.Background(), 1, metric.WithAttributeSet(k8sResolverFailureAttrSet))
			return
		}
		changed := false
		if h.returnNames && h.hostIPs != nil {
			for host, oldIP := range oldEndpoints {
				if newIP, ok := newEndpoints[host]; ok && newIP != oldIP {
					h.logger.Debug("Detected IP change for hostname", zap.String("hostname", host), zap.String("old_ip", oldIP), zap.String("new_ip", newIP))
					h.endpoints.Delete(host)
					h.hostIPs.Delete(host)
					_, _ = h.callback(context.Background())
					changed = true
				}
			}
		}
		// For IP mode we need to detect replacement of IPs (addresses changed entirely).
		for ep := range oldEndpoints {
			if _, ok := newEndpoints[ep]; !ok {
				h.endpoints.Delete(ep)
				if h.returnNames && h.hostIPs != nil {
					h.hostIPs.Delete(ep)
				}
				changed = true
			}
		}
		for ep, ip := range newEndpoints {
			if _, present := h.endpoints.Load(ep); !present {
				h.endpoints.Store(ep, true)
				changed = true
			}
			if h.returnNames && h.hostIPs != nil {
				h.hostIPs.Store(ep, ip)
			}
		}
		if changed {
			_, _ = h.callback(context.Background())
		} else {
			h.logger.Debug("No changes detected in EndpointSlice", zap.Any("old", oldSlice), zap.Any("new", newSlice))
		}
	default:
		h.logger.Warn("Unexpected Kubernetes data type during update", zap.Any("obj", oldObj))
		h.telemetry.LoadbalancerNumResolutions.Add(context.Background(), 1, metric.WithAttributeSet(k8sResolverFailureAttrSet))
		return
	}
}

func (h handler) OnDelete(obj any) {
	var endpoints map[string]string
	var ok bool

	switch object := obj.(type) {
	case *cache.DeletedFinalStateUnknown:
		h.OnDelete(object.Obj)
		return
	case *discoveryv1.EndpointSlice:
		if object != nil {
			ok, endpoints = convertToEndpoints(h.returnNames, object)
			if !ok {
				h.logger.Warn(epMissingHostnamesMsg, zap.Any("obj", obj))
				h.telemetry.LoadbalancerNumResolutions.Add(context.Background(), 1, metric.WithAttributeSet(k8sResolverFailureAttrSet))
				return
			}
		}
	default:
		h.logger.Warn("Unexpected Kubernetes data type during removal", zap.Any("obj", obj))
		h.telemetry.LoadbalancerNumResolutions.Add(context.Background(), 1, metric.WithAttributeSet(k8sResolverFailureAttrSet))
		return
	}
	if len(endpoints) != 0 {
		for endpoint := range endpoints {
			h.endpoints.Delete(endpoint)
			if h.returnNames && h.hostIPs != nil {
				h.hostIPs.Delete(endpoint)
			}
		}
		_, _ = h.callback(context.Background())
	}
}

func convertToEndpoints(retNames bool, slices ...*discoveryv1.EndpointSlice) (bool, map[string]string) {
	res := map[string]string{}
	for _, es := range slices {
		for _, ep := range es.Endpoints {
			// Skip endpoints explicitly marked not ready or not serving. If fields are nil, treat as ready.
			if ep.Conditions.Ready != nil && !*ep.Conditions.Ready {
				continue
			}
			if ep.Conditions.Serving != nil && !*ep.Conditions.Serving {
				continue
			}
			// Each Endpoint may have multiple addresses
			for _, addr := range ep.Addresses {
				if retNames {
					// Hostname field is under EndpointSlice Endpoint
					if ep.Hostname == nil || *ep.Hostname == "" {
						return false, nil
					}
					res[*ep.Hostname] = addr
				} else {
					res[addr] = ""
				}
			}
		}
	}
	return true, res
}
