// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// helper pointer
func strPtr(s string) *string { return &s }

// Smoke test for EndpointSlice-based resolver basic flow + hostname mode.
func TestK8sResolver_Smoke(t *testing.T) {
	service := "lb"
	ns := "default"
	ports := []int32{4317}
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      service + "-slice",
			Namespace: ns,
			Labels:    map[string]string{discoveryv1.LabelServiceName: service},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{"192.168.10.100"}, Hostname: strPtr("pod-0")}},
	}
	cl := fake.NewClientset(slice)
	_, tb := getTelemetryAssets(t)
	res, err := newK8sResolver(cl, zap.NewNop(), service, ports, defaultListWatchTimeout, true, tb)
	require.NoError(t, err)
	require.NoError(t, res.start(t.Context()))
	assert.Equal(t, []string{"pod-0.lb.default:4317"}, res.Endpoints())
	require.NoError(t, res.shutdown(t.Context()))
}

// Config parsing: service name / namespace derivation and error cases.
func Test_newK8sResolver_Config(t *testing.T) {
	tests := []struct {
		name    string
		service string
		wantErr error
		wantNil bool
		wantSvc string
		wantNs  string
	}{
		{name: "empty", service: "", wantErr: errNoSvc, wantNil: true},
		{name: "default ns", service: "lb", wantSvc: "lb", wantNs: "default"},
		{name: "custom ns", service: "lb.other", wantSvc: "lb", wantNs: "other"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, tb := getTelemetryAssets(t)
			res, err := newK8sResolver(fake.NewClientset(), zap.NewNop(), tt.service, []int32{4317}, defaultListWatchTimeout, false, tb)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.wantNil {
				assert.Nil(t, res)
				return
			}
			assert.Equal(t, tt.wantSvc, res.svcName)
			assert.Equal(t, tt.wantNs, res.svcNs)
		})
	}
}
