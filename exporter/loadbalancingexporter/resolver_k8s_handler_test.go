// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func ptrTo(s string) *string { return &s }

func TestConvertToEndpoints(tst *testing.T) {
	// Create dummy EndpointSlice objects
	endpoints1 := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-slice-1",
			Namespace: "test-namespace",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "svc"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{Addresses: []string{"192.168.10.101"}, Hostname: ptrTo("pod-1")},
		},
	}
	endpoints2 := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-slice-2",
			Namespace: "test-namespace",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "svc"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{Addresses: []string{"192.168.10.102"}, Hostname: ptrTo("pod-2")},
		},
	}
	endpoints3 := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-slice-3",
			Namespace: "test-namespace",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "svc"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{Addresses: []string{"192.168.10.103"}},
		},
	}

	tests := []struct {
		name              string
		returnNames       bool
		includedEndpoints []*discoveryv1.EndpointSlice
		expectedEndpoints map[string]string
		wantNil           bool
	}{
		{
			name:              "return hostnames",
			returnNames:       true,
			includedEndpoints: []*discoveryv1.EndpointSlice{endpoints1, endpoints2},
			expectedEndpoints: map[string]string{"pod-1": "192.168.10.101", "pod-2": "192.168.10.102"},
			wantNil:           false,
		},
		{
			name:              "return IPs",
			returnNames:       false,
			includedEndpoints: []*discoveryv1.EndpointSlice{endpoints1, endpoints2, endpoints3},
			expectedEndpoints: map[string]string{"192.168.10.101": "", "192.168.10.102": "", "192.168.10.103": ""},
			wantNil:           false,
		},
		{
			name:              "missing hostname",
			returnNames:       true,
			includedEndpoints: []*discoveryv1.EndpointSlice{endpoints1, endpoints3},
			expectedEndpoints: nil,
			wantNil:           true,
		},
	}

	for _, tt := range tests {
		tst.Run(tt.name, func(tst *testing.T) {
			ok, res := convertToEndpoints(tt.returnNames, tt.includedEndpoints...)
			if tt.wantNil {
				assert.Nil(tst, res)
			} else {
				assert.Equal(tst, tt.expectedEndpoints, res)
			}
			assert.Equal(tst, !tt.wantNil, ok)
		})
	}
}

func TestConvertToEndpoints_ConditionsFiltering(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }

	// Endpoints with various readiness/serving combinations
	epReady := discoveryv1.Endpoint{Addresses: []string{"10.0.0.1"}, Hostname: ptrTo("pod-ready"), Conditions: discoveryv1.EndpointConditions{Ready: boolPtr(true)}}
	epNotReady := discoveryv1.Endpoint{Addresses: []string{"10.0.0.2"}, Hostname: ptrTo("pod-notready"), Conditions: discoveryv1.EndpointConditions{Ready: boolPtr(false)}}
	epServingFalse := discoveryv1.Endpoint{Addresses: []string{"10.0.0.4"}, Hostname: ptrTo("pod-serving-false"), Conditions: discoveryv1.EndpointConditions{Ready: boolPtr(true), Serving: boolPtr(false)}}
	epNilConditions := discoveryv1.Endpoint{Addresses: []string{"10.0.0.3"}, Hostname: ptrTo("pod-nil")}

	slice := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Name: "svc-slice", Namespace: "ns", Labels: map[string]string{"kubernetes.io/service-name": "svc"}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   []discoveryv1.Endpoint{epReady, epNotReady, epNilConditions, epServingFalse},
	}

	// returnNames=false (IP mode): expect only ready & nilConditions IPs
	ok, res := convertToEndpoints(false, slice)
	assert.True(t, ok)
	assert.Equal(t, map[string]string{"10.0.0.1": "", "10.0.0.3": ""}, res)

	// returnNames=true (hostname mode): expect only hostnames for ready & nilConditions endpoints
	ok, res = convertToEndpoints(true, slice)
	assert.True(t, ok)
	assert.Equal(t, map[string]string{"pod-ready": "10.0.0.1", "pod-nil": "10.0.0.3"}, res)
}
