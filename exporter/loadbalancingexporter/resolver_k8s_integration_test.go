// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	context "context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// waitForCondition polls fn until it returns true or timeout expires.
func waitForCondition(t *testing.T, timeout, interval time.Duration, fn func(ctx context.Context) (bool, error)) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	for {
		ok, err := fn(ctx)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func newSlice(name, namespace, svc string, endpoints ...discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{discoveryv1.LabelServiceName: svc},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   endpoints,
	}
}

func strP(s string) *string { return &s }

// Test multi-slice add/remove and aggregation in IP mode (returnNames = false).
func TestK8sResolver_MultiSlice_AddRemove(t *testing.T) {
	service := "lb"
	ns := "default"
	ports := []int32{4317}

	// initial slice with one endpoint
	slice1 := newSlice(service+"-slice-1", ns, service, discoveryv1.Endpoint{Addresses: []string{"10.0.0.1"}})
	cl := fake.NewClientset(slice1)
	_, tb := getTelemetryAssets(t)
	res, err := newK8sResolver(cl, zap.NewNop(), service, ports, defaultListWatchTimeout, false, tb)
	require.NoError(t, err)
	require.NoError(t, res.start(t.Context()))
	defer res.shutdown(t.Context())

	// wait initial
	require.NoError(t, waitForCondition(t, 2*time.Second, 25*time.Millisecond, func(ctx context.Context) (bool, error) {
		_, _ = res.resolve(ctx)
		return assert.ElementsMatchf(t, []string{"10.0.0.1:4317"}, res.Endpoints(), "initial endpoints mismatch"), nil
	}))

	// add second slice via direct handler call to avoid fake watch flakiness
	slice2 := newSlice(service+"-slice-2", ns, service, discoveryv1.Endpoint{Addresses: []string{"10.0.0.2"}})
	res.handler.OnAdd(slice2, false)

	require.NoError(t, waitForCondition(t, 2*time.Second, 25*time.Millisecond, func(ctx context.Context) (bool, error) {
		_, _ = res.resolve(ctx)
		exp := []string{"10.0.0.1:4317", "10.0.0.2:4317"}
		got := append([]string(nil), res.Endpoints()...)
		sort.Strings(got)
		return assert.EqualValues(t, exp, got), nil
	}))

	// delete first slice via handler
	res.handler.OnDelete(slice1)
	require.NoError(t, waitForCondition(t, 2*time.Second, 25*time.Millisecond, func(ctx context.Context) (bool, error) {
		_, _ = res.resolve(ctx)
		return assert.ElementsMatch(t, []string{"10.0.0.2:4317"}, res.Endpoints()), nil
	}))
}

// Test IP change while hostname stays stable triggers a callback (returnNames = true).
func TestK8sResolver_IPChangeWithStableHostname_CallbackTriggered(t *testing.T) {
	service := "lb"
	ns := "default"
	ports := []int32{4317}
	slice := newSlice(service+"-slice", ns, service, discoveryv1.Endpoint{Addresses: []string{"10.0.0.5"}, Hostname: strP("pod-0")})
	cl := fake.NewClientset(slice)
	_, tb := getTelemetryAssets(t)
	res, err := newK8sResolver(cl, zap.NewNop(), service, ports, defaultListWatchTimeout, true, tb)
	require.NoError(t, err)

	callbackCount := 0
	res.onChange(func(_ []string) { callbackCount++ })

	require.NoError(t, res.start(t.Context()))
	defer res.shutdown(t.Context())

	// wait initial population
	require.NoError(t, waitForCondition(t, 2*time.Second, 25*time.Millisecond, func(ctx context.Context) (bool, error) {
		_, _ = res.resolve(ctx)
		return len(res.Endpoints()) == 1, nil
	}))
	initialCallbacks := callbackCount

	// update the slice with same hostname new IP
	updated := slice.DeepCopy()
	updated.Endpoints = []discoveryv1.Endpoint{{Addresses: []string{"10.0.0.99"}, Hostname: strP("pod-0")}}
	res.handler.OnUpdate(slice, updated)

	require.NoError(t, waitForCondition(t, 2*time.Second, 25*time.Millisecond, func(ctx context.Context) (bool, error) {
		_, _ = res.resolve(ctx)
		return callbackCount > initialCallbacks, nil
	}))
	// Endpoints list should be same logically (hostname based)
	assert.Equal(t, []string{"pod-0.lb.default:4317"}, res.Endpoints())
}

// Test IP change in IP mode (returnNames = false) updates endpoint list.
func TestK8sResolver_IPChange_IPMode(t *testing.T) {
	service := "lb"
	ns := "default"
	ports := []int32{4317}
	slice := newSlice(service+"-slice", ns, service, discoveryv1.Endpoint{Addresses: []string{"10.1.0.1"}, Hostname: strP("pod-0")})
	cl := fake.NewClientset(slice)
	_, tb := getTelemetryAssets(t)
	res, err := newK8sResolver(cl, zap.NewNop(), service, ports, defaultListWatchTimeout, false, tb)
	require.NoError(t, err)
	require.NoError(t, res.start(t.Context()))
	defer res.shutdown(t.Context())

	require.NoError(t, waitForCondition(t, 2*time.Second, 25*time.Millisecond, func(ctx context.Context) (bool, error) {
		_, _ = res.resolve(ctx)
		return assert.ElementsMatch(t, []string{"10.1.0.1:4317"}, res.Endpoints()), nil
	}))

	updated := slice.DeepCopy()
	updated.Endpoints = []discoveryv1.Endpoint{{Addresses: []string{"10.1.0.2"}, Hostname: strP("pod-0")}}
	res.handler.OnUpdate(slice, updated)

	require.NoError(t, waitForCondition(t, 2*time.Second, 25*time.Millisecond, func(ctx context.Context) (bool, error) {
		_, _ = res.resolve(ctx)
		return assert.ElementsMatch(t, []string{"10.1.0.2:4317"}, res.Endpoints()), nil
	}))
}
