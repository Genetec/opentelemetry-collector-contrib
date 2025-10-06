// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// Test that the shared informer path (List+Watch) processes adds & updates using a fake reactor.
func TestK8sResolver_InformerWatchFlow(t *testing.T) {
	service := "lb"
	ns := "default"
	ports := []int32{4317}

	initial := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:            service + "-slice-1",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels:          map[string]string{discoveryv1.LabelServiceName: service},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{"10.2.0.1"}}},
	}

	client := fake.NewClientset(initial)

	// We'll intercept watch calls and feed events manually.
	w := watch.NewFake()
	client.Fake.PrependWatchReactor("endpointslices", func(action ktesting.Action) (handled bool, ret watch.Interface, err error) {
		return true, w, nil
	})

	_, tb := getTelemetryAssets(t)
	resolver, err := newK8sResolver(client, zap.NewNop(), service, ports, defaultListWatchTimeout, false, tb)
	require.NoError(t, err)
	require.NoError(t, resolver.start(t.Context()))
	defer resolver.shutdown(t.Context())

	// Wait initial list processed
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, _ = resolver.resolve(context.Background())
		if len(resolver.Endpoints()) == 1 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	assert.ElementsMatch(t, []string{"10.2.0.1:4317"}, resolver.Endpoints())

	// Simulate a new slice add through watch
	second := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:            service + "-slice-2",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels:          map[string]string{discoveryv1.LabelServiceName: service},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{"10.2.0.2"}}},
	}
	w.Add(second)

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, _ = resolver.resolve(context.Background())
		if len(resolver.Endpoints()) == 2 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	assert.ElementsMatch(t, []string{"10.2.0.1:4317", "10.2.0.2:4317"}, resolver.Endpoints())

	// Update first slice IP using handler directly for determinism (avoid watch Modify race)
	updated := initial.DeepCopy()
	updated.ResourceVersion = "2"
	updated.Endpoints = []discoveryv1.Endpoint{{Addresses: []string{"10.2.0.3"}}}
	resolver.handler.OnUpdate(initial, updated)
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, _ = resolver.resolve(context.Background())
		if assert.ElementsMatch(t, []string{"10.2.0.2:4317", "10.2.0.3:4317"}, resolver.Endpoints()) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	assert.ElementsMatch(t, []string{"10.2.0.2:4317", "10.2.0.3:4317"}, resolver.Endpoints())
}
