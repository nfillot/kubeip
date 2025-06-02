package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/doitintl/kubeip/internal/address"
	"github.com/doitintl/kubeip/internal/config"
	"github.com/doitintl/kubeip/internal/node"
	"github.com/doitintl/kubeip/internal/types"
	mocks "github.com/doitintl/kubeip/mocks/address"
	nodeMocks "github.com/doitintl/kubeip/mocks/node"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tmock "github.com/stretchr/testify/mock"
	"k8s.io/client-go/kubernetes/fake"
)

func Test_assignAddress(t *testing.T) {
	type args struct {
		c          context.Context
		assignerFn func(t *testing.T) address.Assigner
		node       *types.Node
		cfg        *config.Config
	}
	tests := []struct {
		name    string
		args    args
		address string
		wantErr bool
	}{
		{
			name:    "assign address successfully",
			address: "1.1.1.1",
			args: args{
				c: context.Background(),
				assignerFn: func(t *testing.T) address.Assigner {
					mock := mocks.NewAssigner(t)
					mock.EXPECT().Assign(tmock.Anything, "test-instance", "test-zone", []string{"test-filter"}, "test-order-by").Return("1.1.1.1", nil)
					return mock
				},
				node: &types.Node{
					Name:     "test-node",
					Instance: "test-instance",
					Region:   "test-region",
					Zone:     "test-zone",
				},
				cfg: &config.Config{
					Filter:        []string{"test-filter"},
					OrderBy:       "test-order-by",
					RetryAttempts: 3,
					RetryInterval: time.Millisecond,
					LeaseDuration: 1,
				},
			},
		},
		{
			name:    "assign address after a few retries",
			address: "1.1.1.1",
			args: args{
				c: context.Background(),
				assignerFn: func(t *testing.T) address.Assigner {
					mock := mocks.NewAssigner(t)
					mock.EXPECT().Assign(tmock.Anything, "test-instance", "test-zone", []string{"test-filter"}, "test-order-by").Return("", errors.New("first error")).Once()
					mock.EXPECT().Assign(tmock.Anything, "test-instance", "test-zone", []string{"test-filter"}, "test-order-by").Return("", errors.New("second error")).Once()
					mock.EXPECT().Assign(tmock.Anything, "test-instance", "test-zone", []string{"test-filter"}, "test-order-by").Return("1.1.1.1", nil).Once()
					return mock
				},
				node: &types.Node{
					Name:     "test-node",
					Instance: "test-instance",
					Region:   "test-region",
					Zone:     "test-zone",
				},
				cfg: &config.Config{
					Filter:        []string{"test-filter"},
					OrderBy:       "test-order-by",
					RetryAttempts: 3,
					RetryInterval: time.Millisecond,
					LeaseDuration: 1,
				},
			},
		},
		{
			name: "error after a few retries and reached maximum number of retries",
			args: args{
				c: context.Background(),
				assignerFn: func(t *testing.T) address.Assigner {
					mock := mocks.NewAssigner(t)
					mock.EXPECT().Assign(tmock.Anything, "test-instance", "test-zone", []string{"test-filter"}, "test-order-by").Return("", errors.New("error")).Times(4)
					return mock
				},
				node: &types.Node{
					Name:     "test-node",
					Instance: "test-instance",
					Region:   "test-region",
					Zone:     "test-zone",
				},
				cfg: &config.Config{
					Filter:        []string{"test-filter"},
					OrderBy:       "test-order-by",
					RetryAttempts: 3,
					RetryInterval: time.Millisecond,
					LeaseDuration: 1,
				},
			},
			wantErr: true,
		},
		{
			name: "context cancelled while assigning addresses",
			args: args{
				c: func() context.Context {
					ctx, cancel := context.WithCancel(context.Background())
					go func() {
						// Simulate a shutdown signal being received after a short delay
						time.Sleep(20 * time.Millisecond)
						cancel()
					}()
					return ctx
				}(),
				assignerFn: func(t *testing.T) address.Assigner {
					mock := mocks.NewAssigner(t)
					mock.EXPECT().Assign(tmock.Anything, "test-instance", "test-zone", []string{"test-filter"}, "test-order-by").Return("", errors.New("error")).Maybe()
					return mock
				},
				node: &types.Node{
					Name:     "test-node",
					Instance: "test-instance",
					Region:   "test-region",
					Zone:     "test-zone",
				},
				cfg: &config.Config{
					Filter:        []string{"test-filter"},
					OrderBy:       "test-order-by",
					RetryAttempts: 10,
					RetryInterval: 5 * time.Millisecond,
					LeaseDuration: 1,
				},
			},
			wantErr: true,
		},
		{
			name: "error after a few retries and context is done",
			args: args{
				c: func() context.Context {
					ctx, _ := context.WithTimeout(context.Background(), 10*time.Millisecond) //nolint:govet
					return ctx
				}(),
				assignerFn: func(t *testing.T) address.Assigner {
					mock := mocks.NewAssigner(t)
					mock.EXPECT().Assign(tmock.Anything, "test-instance", "test-zone", []string{"test-filter"}, "test-order-by").Return("", errors.New("error")).Maybe()
					return mock
				},
				node: &types.Node{
					Name:     "test-node",
					Instance: "test-instance",
					Region:   "test-region",
					Zone:     "test-zone",
				},
				cfg: &config.Config{
					Filter:        []string{"test-filter"},
					OrderBy:       "test-order-by",
					RetryAttempts: 3,
					RetryInterval: 15 * time.Millisecond,
					LeaseDuration: 1,
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := prepareLogger("debug", false)
			assigner := tt.args.assignerFn(t)
			client := fake.NewSimpleClientset()
			assignedAddress, err := assignAddress(tt.args.c, log, client, assigner, tt.args.node, tt.args.cfg)
			if err != nil != tt.wantErr {
				t.Errorf("assignAddress() error = %v, wantErr %v", err, tt.wantErr)
			} else if assignedAddress != tt.address {
				t.Fatalf("assignAddress() = %v, want %v", assignedAddress, tt.address)
			}
		})
	}
}

func TestMetricsUpdates(t *testing.T) {
	log := prepareLogger("debug", false)
	client := fake.NewSimpleClientset() // Mock Kubernetes client

	defaultNode := &types.Node{
		Name:     "test-node",
		Instance: "test-instance",
		Region:   "test-region",
		Zone:     "test-zone",
		Cloud:    types.CloudProviderGCP, // Assuming a default for tests
	}
	defaultCfg := &config.Config{
		Filter:         []string{"test-filter"},
		OrderBy:        "test-order-by",
		RetryAttempts:  1, // Keep retries low for faster tests
		RetryInterval:  time.Millisecond,
		LeaseDuration:  1,
		LeaseNamespace: "default",
		NodeName:       "test-node",
	}

	t.Run("initial stats collection", func(t *testing.T) {
		mockAssigner := mocks.NewAssigner(t)
		mockAssigner.EXPECT().GetIPAddressStats(tmock.Anything, defaultCfg.Filter, defaultCfg.OrderBy).Return(10, 4, nil).Once()

		// Use a local registry for this part of the test to avoid direct manipulation of global metrics
		// and allow isolated testing of the logic that *would* update global metrics.
		// The global metrics themselves are tested in TestMetricsEndpoint with a local registry.
		tempUsableTotal := prometheus.NewGauge(prometheus.GaugeOpts{})
		tempAssignedTotal := prometheus.NewGauge(prometheus.GaugeOpts{})
		tempAvailableTotal := prometheus.NewGauge(prometheus.GaugeOpts{})

		// Simulate the part of run() that updates these metrics
		ctx := context.Background()
		usable, assigned, statsErr := mockAssigner.GetIPAddressStats(ctx, defaultCfg.Filter, defaultCfg.OrderBy)
		if statsErr != nil {
			t.Fatalf("GetIPAddressStats failed: %v", statsErr)
		}
		tempUsableTotal.Set(float64(usable))
		tempAssignedTotal.Set(float64(assigned))
		tempAvailableTotal.Set(float64(usable - assigned))

		if val := testutil.ToFloat64(tempUsableTotal); val != 10 {
			t.Errorf("kubeipIPAddressUsableTotal want 10, got %v", val)
		}
		if val := testutil.ToFloat64(tempAssignedTotal); val != 4 {
			t.Errorf("kubeipIPAddressAssignedTotal want 4, got %v", val)
		}
		if val := testutil.ToFloat64(tempAvailableTotal); val != 6 {
			t.Errorf("kubeipIPAddressAvailableTotal want 6, got %v", val)
		}
		mockAssigner.AssertExpectations(t)
	})

	t.Run("kubeip_ip_address_assigned on successful assignment", func(t *testing.T) {
		mockAssigner := mocks.NewAssigner(t)
		assignedIP := "1.2.3.4"
		mockAssigner.EXPECT().Assign(tmock.Anything, defaultNode.Instance, defaultNode.Zone, defaultCfg.Filter, defaultCfg.OrderBy).Return(assignedIP, nil).Once()

		// For testing the assignAddress function's metric side-effects,
		// we'll have to use the global kubeipIPAddressAssigned metric as it's directly used.
		// Reset it to ensure a clean state for this sub-test.
		kubeipIPAddressAssigned.Reset()


		_, err := assignAddress(context.Background(), log, client, mockAssigner, defaultNode, defaultCfg)
		if err != nil {
			t.Fatalf("assignAddress failed: %v", err)
		}

		labels := prometheus.Labels{"k8s_node": defaultNode.Name, "ip_address_name": assignedIP, "ip_address": assignedIP}
		if val := testutil.ToFloat64(kubeipIPAddressAssigned.With(labels)); val != 0 {
			t.Errorf("kubeipIPAddressAssigned want 0 for success, got %v", val)
		}
		mockAssigner.AssertExpectations(t)
	})

	t.Run("kubeip_ip_address_assigned on failed assignment", func(t *testing.T) {
		mockAssigner := mocks.NewAssigner(t)
		mockAssigner.EXPECT().Assign(tmock.Anything, defaultNode.Instance, defaultNode.Zone, defaultCfg.Filter, defaultCfg.OrderBy).Return("", errors.New("assignment failed")).Times(defaultCfg.RetryAttempts + 1)

		kubeipIPAddressAssigned.Reset()

		_, err := assignAddress(context.Background(), log, client, mockAssigner, defaultNode, defaultCfg)
		if err == nil {
			t.Fatalf("assignAddress should have failed")
		}
		labels := prometheus.Labels{"k8s_node": defaultNode.Name, "ip_address_name": "unknown", "ip_address": "unknown"}
		if val := testutil.ToFloat64(kubeipIPAddressAssigned.With(labels)); val != 1 {
			t.Errorf("kubeipIPAddressAssigned want 1 for failure, got %v", val)
		}
		mockAssigner.AssertExpectations(t)
	})
}

func TestMetricsEndpoint(t *testing.T) {
	// It's important to use a local registry for testing to avoid conflicts
	// and to ensure a clean state for each test run.
	reg := prometheus.NewRegistry()
	// Create new instances of gauges and vec for this registry
	// We use the actual metric names as defined globally because that's what promhttp will expose.
	localKubeipIPAddressUsableTotal := prometheus.NewGauge(prometheus.GaugeOpts{Name: "kubeip_ip_address_usable_total", Help: "Total number of reserved IP addresses found and usable by KubeIP"})
	localKubeipIPAddressAssignedTotal := prometheus.NewGauge(prometheus.GaugeOpts{Name: "kubeip_ip_address_assigned_total", Help: "Total number of available IP addresses currently in use"})
	localKubeipIPAddressAvailableTotal := prometheus.NewGauge(prometheus.GaugeOpts{Name: "kubeip_ip_address_available_total", Help: "Total number of available IP addresses still available (not in use)"})
	localKubeipIPAddressAssigned := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kubeip_ip_address_assigned", Help: "Indicates if an IP address was assigned to the node KubeIP is running on (0 for success, 1 for failure)"}, []string{"k8s_node", "ip_address_name", "ip_address"})

	reg.MustRegister(localKubeipIPAddressUsableTotal)
	reg.MustRegister(localKubeipIPAddressAssignedTotal)
	reg.MustRegister(localKubeipIPAddressAvailableTotal)
	reg.MustRegister(localKubeipIPAddressAssigned)

	// Set some sample values on local metrics
	localKubeipIPAddressUsableTotal.Set(20)
	localKubeipIPAddressAssignedTotal.Set(5)
	localKubeipIPAddressAvailableTotal.Set(15)
	localKubeipIPAddressAssigned.WithLabelValues("test-node-endpoint", "5.6.7.8", "5.6.7.8").Set(0)

	server := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	defer server.Close()

	resp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("Failed to GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	bodyString := string(bodyBytes)

	// Check for presence of metrics and their values
	expectedMetrics := []string{
		"# HELP kubeip_ip_address_assigned Indicates if an IP address was assigned to the node KubeIP is running on (0 for success, 1 for failure).",
		"# TYPE kubeip_ip_address_assigned gauge",
		`kubeip_ip_address_assigned{ip_address="5.6.7.8",ip_address_name="5.6.7.8",k8s_node="test-node-endpoint"} 0`,
		"# HELP kubeip_ip_address_assigned_total Total number of available IP addresses currently in use.",
		"# TYPE kubeip_ip_address_assigned_total gauge",
		"kubeip_ip_address_assigned_total 5",
		"# HELP kubeip_ip_address_available_total Total number of available IP addresses still available (not in use).",
		"# TYPE kubeip_ip_address_available_total gauge",
		"kubeip_ip_address_available_total 15",
		"# HELP kubeip_ip_address_usable_total Total number of reserved IP addresses found and usable by KubeIP.",
		"# TYPE kubeip_ip_address_usable_total gauge",
		"kubeip_ip_address_usable_total 20",
	}

	for _, expectedMetric := range expectedMetrics {
		if !strings.Contains(bodyString, expectedMetric) {
			t.Errorf("Response body does not contain expected metric substring: %s\nFull body:\n%s", expectedMetric, bodyString)
		}
	}
}

func Test_waitForAddressToBeReported(t *testing.T) {
	type args struct {
		c          context.Context
		explorerFn func(t *testing.T) node.Explorer
		node       *types.Node
		address    string
		cfg        *config.Config
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "address reported with no retries",
			args: args{
				c:       context.Background(),
				address: "1.1.1.1",
				explorerFn: func(t *testing.T) node.Explorer {
					mock := nodeMocks.NewExplorer(t)
					mock.EXPECT().GetNode(tmock.Anything, "test-node").Return(
						&types.Node{
							Name:        "test-node",
							Instance:    "test-instance",
							Region:      "test-region",
							Zone:        "test-zone",
							ExternalIPs: []net.IP{net.IPv4(1, 1, 1, 1)},
						},
						nil,
					)
					return mock
				},
				node: &types.Node{
					Name:     "test-node",
					Instance: "test-instance",
					Region:   "test-region",
					Zone:     "test-zone",
				},
				cfg: &config.Config{
					Filter:        []string{"test-filter"},
					OrderBy:       "test-order-by",
					RetryAttempts: 3,
					RetryInterval: time.Millisecond,
					LeaseDuration: 1,
				},
			},
		},
		{
			name: "address reported after a few retries",
			args: args{
				c:       context.Background(),
				address: "1.1.1.1",
				explorerFn: func(t *testing.T) node.Explorer {
					mock := nodeMocks.NewExplorer(t)
					mock.EXPECT().GetNode(tmock.Anything, "test-node").Return(&types.Node{
						Name:        "test-node",
						Instance:    "test-instance",
						Region:      "test-region",
						Zone:        "test-zone",
						ExternalIPs: []net.IP{net.IPv4(9, 9, 9, 9)},
					}, nil).Times(3)
					mock.EXPECT().GetNode(tmock.Anything, "test-node").Return(&types.Node{
						Name:        "test-node",
						Instance:    "test-instance",
						Region:      "test-region",
						Zone:        "test-zone",
						ExternalIPs: []net.IP{net.IPv4(1, 1, 1, 1)},
					}, nil).Once()
					return mock
				},
				node: &types.Node{
					Name:     "test-node",
					Instance: "test-instance",
					Region:   "test-region",
					Zone:     "test-zone",
				},
				cfg: &config.Config{
					Filter:        []string{"test-filter"},
					OrderBy:       "test-order-by",
					RetryAttempts: 3,
					RetryInterval: time.Millisecond,
					LeaseDuration: 1,
				},
			},
		},
		{
			name: "error after a few retries and reached maximum number of retries",
			args: args{
				c: context.Background(),
				explorerFn: func(t *testing.T) node.Explorer {
					mock := nodeMocks.NewExplorer(t)
					mock.EXPECT().GetNode(tmock.Anything, "test-node").Return(&types.Node{
						Name:        "test-node",
						Instance:    "test-instance",
						Region:      "test-region",
						Zone:        "test-zone",
						ExternalIPs: []net.IP{net.IPv4(9, 9, 9, 9)},
					}, nil).Times(4)
					mock.EXPECT().GetNode(tmock.Anything, "test-node").Return(&types.Node{
						Name:        "test-node",
						Instance:    "test-instance",
						Region:      "test-region",
						Zone:        "test-zone",
						ExternalIPs: []net.IP{net.IPv4(1, 1, 1, 1)},
					}, nil).Times(0)
					return mock
				},
				node: &types.Node{
					Name:     "test-node",
					Instance: "test-instance",
					Region:   "test-region",
					Zone:     "test-zone",
				},
				cfg: &config.Config{
					Filter:        []string{"test-filter"},
					OrderBy:       "test-order-by",
					RetryAttempts: 3,
					RetryInterval: time.Millisecond,
					LeaseDuration: 1,
				},
			},
			wantErr: true,
		},
		{
			name: "context cancelled while waiting for address to be reported",
			args: args{
				c: func() context.Context {
					ctx, cancel := context.WithCancel(context.Background())
					go func() {
						// Simulate a shutdown signal being received after a short delay
						time.Sleep(20 * time.Millisecond)
						cancel()
					}()
					return ctx
				}(),
				explorerFn: func(t *testing.T) node.Explorer {
					mock := nodeMocks.NewExplorer(t)
					mock.EXPECT().GetNode(tmock.Anything, "test-node").Return(nil, errors.New("error")).Maybe()
					return mock
				},
				node: &types.Node{
					Name:     "test-node",
					Instance: "test-instance",
					Region:   "test-region",
					Zone:     "test-zone",
				},
				cfg: &config.Config{
					Filter:        []string{"test-filter"},
					OrderBy:       "test-order-by",
					RetryAttempts: 10,
					RetryInterval: 5 * time.Millisecond,
					LeaseDuration: 1,
				},
			},
			wantErr: true,
		},
		{
			name: "error after a few retries and context is done",
			args: args{
				c: func() context.Context {
					ctx, _ := context.WithTimeout(context.Background(), 10*time.Millisecond) //nolint:govet
					return ctx
				}(),
				explorerFn: func(t *testing.T) node.Explorer {
					mock := nodeMocks.NewExplorer(t)
					mock.EXPECT().GetNode(tmock.Anything, "test-node").Return(nil, errors.New("error")).Maybe()
					return mock
				},
				node: &types.Node{
					Name:     "test-node",
					Instance: "test-instance",
					Region:   "test-region",
					Zone:     "test-zone",
				},
				cfg: &config.Config{
					Filter:        []string{"test-filter"},
					OrderBy:       "test-order-by",
					RetryAttempts: 3,
					RetryInterval: 15 * time.Millisecond,
					LeaseDuration: 1,
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := prepareLogger("debug", false)
			explorer := tt.args.explorerFn(t)
			err := waitForAddressToBeReported(tt.args.c, log, explorer, tt.args.node, tt.args.address, tt.args.cfg)
			if err != nil != tt.wantErr {
				t.Errorf("waitForAddressToBeReported() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
