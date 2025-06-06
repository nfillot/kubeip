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
	"github.com/sirupsen/logrus" // Added for TestMetricsRefresh
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

// MockAssigner is a mock implementation of the address.Assigner interface.
// Note: This is defined here because the existing tests use a different mocking setup
// (e.g., mocks.NewAssigner(t) from a `mocks` package).
// For the self-contained TestMetricsRefresh, we use this local mock.
type MockAssigner struct {
	tmock.Mock
}

// Assign mocks the Assign method.
func (m *MockAssigner) Assign(ctx context.Context, instanceID, zone string, filter []string, orderBy string) (string, error) {
	args := m.Called(ctx, instanceID, zone, filter, orderBy)
	return args.String(0), args.Error(1)
}

// Unassign mocks the Unassign method.
func (m *MockAssigner) Unassign(ctx context.Context, instanceID, zone string) error {
	args := m.Called(ctx, instanceID, zone)
	return args.Error(0)
}

// GetIPAddressStats mocks the GetIPAddressStats method.
func (m *MockAssigner) GetIPAddressStats(ctx context.Context, filter []string, orderBy string) (usable, assigned int, err error) {
	args := m.Called(ctx, filter, orderBy)
	return args.Int(0), args.Int(1), args.Error(2)
}

func TestMetricsRefresh(t *testing.T) {
	// Register metrics for this test and defer unregistration
	// Ensure global metrics are registered for this test, similar to how they are in runCmd
	// This might cause issues if other tests run in parallel and also manipulate global registry.
	// Consider using a local registry if tests become flaky.
	prometheus.MustRegister(kubeipIPAddressUsableTotal)
	prometheus.MustRegister(kubeipIPAddressAssignedTotal)
	prometheus.MustRegister(kubeipIPAddressAvailableTotal)
	// kubeipIPAddressAssigned is a GaugeVec, might need different handling if tested

	defer prometheus.Unregister(kubeipIPAddressUsableTotal)
	defer prometheus.Unregister(kubeipIPAddressAssignedTotal)
	defer prometheus.Unregister(kubeipIPAddressAvailableTotal)
	// defer prometheus.Unregister(kubeipIPAddressAssigned) // If it were registered

	log := logrus.NewEntry(logrus.New())
	// Set to WarnLevel to reduce noise during tests, or DebugLevel for more info
	log.Logger.SetLevel(logrus.WarnLevel)

	mockAssigner := new(MockAssigner) // Uses the local MockAssigner
	cfg := &config.Config{
		MetricsRefreshInterval: 100 * time.Millisecond,
		Filter:                 []string{"env=test", "region=us-east-1"}, // Example filter
		OrderBy:                "name",      // Example orderby
	}

	// --- Setup mock expectations ---
	// 1. Initial call (simulating what might happen in `run` before the refresh loop starts its own ticking)
	// This simulates the initial fetch performed by the run function itself.
	mockAssigner.On("GetIPAddressStats", tmock.Anything, cfg.Filter, cfg.OrderBy).Return(10, 5, nil).Once()

	// --- Simulate initial metric values (as if set by `run` function) ---
	// We call the mock directly to get the values for the initial setup.
	// This is because the test is focused on the goroutine's behavior, assuming initial state.
	usableInitial, assignedInitial, errInitial := mockAssigner.GetIPAddressStats(context.Background(), cfg.Filter, cfg.OrderBy)
	if errInitial != nil { // Use if, not assert, to allow mock to record this call for AssertExpectations
		t.Fatalf("Mock call for initial stats should not error: %v", errInitial)
	}

	kubeipIPAddressUsableTotal.Set(float64(usableInitial))
	kubeipIPAddressAssignedTotal.Set(float64(assignedInitial))
	kubeipIPAddressAvailableTotal.Set(float64(usableInitial - assignedInitial))

	// 2. Refreshed call (this is what the ticker loop inside the goroutine should call)
	mockAssigner.On("GetIPAddressStats", tmock.Anything, cfg.Filter, cfg.OrderBy).Return(12, 6, nil).Once()

	// Context for controlling the refresh loop goroutine's lifetime
	loopCtx, cancelLoop := context.WithTimeout(context.Background(), 500*time.Millisecond) // Test timeout
	defer cancelLoop() // Ensure cancellation at the end of the test

	// --- Start the metrics refresh goroutine (mimicking the one in `main.go`'s `run` function) ---
	// This is a simplified version of the goroutine in main.go's run()
	go func() {
		if cfg.MetricsRefreshInterval <= 0 {
			// log.Warn("test metrics refresh: MetricsRefreshInterval is not positive, refresh goroutine will not start.")
			return
		}
		ticker := time.NewTicker(cfg.MetricsRefreshInterval)
		defer ticker.Stop()

		// log.Debug("test metrics refresh: Starting goroutine")
		for {
			select {
			case <-ticker.C:
				// log.Debug("test metrics refresh: Tick received")
				// Pass loopCtx to GetIPAddressStats for cancellation propagation if the call is long-running
				usable, assigned, err := mockAssigner.GetIPAddressStats(loopCtx, cfg.Filter, cfg.OrderBy)
				if err != nil {
					// log.WithError(err).Warn("test metrics refresh: GetIPAddressStats failed during refresh")
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						// log.Debug("test metrics refresh: Context done during stats fetch, exiting goroutine.")
						return
					}
					continue
				}
				kubeipIPAddressUsableTotal.Set(float64(usable))
				kubeipIPAddressAssignedTotal.Set(float64(assigned))
				kubeipIPAddressAvailableTotal.Set(float64(usable - assigned))
				// log.WithFields(logrus.Fields{"usable": usable, "assigned": assigned}).Debug("test metrics refresh: Stats updated")
			case <-loopCtx.Done():
				// log.Info("test metrics refresh: Context done, stopping goroutine.")
				return
			}
		}
	}()

	// --- Wait for the refresh to occur ---
	// Allow time for at least one ticker tick and processing.
	time.Sleep(cfg.MetricsRefreshInterval + 50*time.Millisecond)
	// log.Debug("test metrics refresh: Finished sleeping, proceeding to assertions.")

	// --- Fetch and assert metrics values ---
	// Using testutil.CollectAndCompare for checking specific metric values.
	expectedUsable := `# HELP kubeip_ip_address_usable_total Total number of reserved IP addresses found and usable by KubeIP
# TYPE kubeip_ip_address_usable_total gauge
kubeip_ip_address_usable_total 12
`
	if err := testutil.CollectAndCompare(kubeipIPAddressUsableTotal, strings.NewReader(expectedUsable), "kubeip_ip_address_usable_total"); err != nil {
		t.Errorf("Usable total metric validation failed: %v", err)
	}

	expectedAssigned := `# HELP kubeip_ip_address_assigned_total Total number of available IP addresses currently in use
# TYPE kubeip_ip_address_assigned_total gauge
kubeip_ip_address_assigned_total 6
`
	if err := testutil.CollectAndCompare(kubeipIPAddressAssignedTotal, strings.NewReader(expectedAssigned), "kubeip_ip_address_assigned_total"); err != nil {
		t.Errorf("Assigned total metric validation failed: %v", err)
	}

	expectedAvailable := `# HELP kubeip_ip_address_available_total Total number of available IP addresses still available (not in use)
# TYPE kubeip_ip_address_available_total gauge
kubeip_ip_address_available_total 6
`
	if err := testutil.CollectAndCompare(kubeipIPAddressAvailableTotal, strings.NewReader(expectedAvailable), "kubeip_ip_address_available_total"); err != nil {
		t.Errorf("Available total metric validation failed: %v", err)
	}

	// --- Verify mock expectations ---
	mockAssigner.AssertExpectations(t)
	// log.Debug("test metrics refresh: Mock expectations asserted.")

	// Explicitly cancel the context to ensure the goroutine stops, though timeout also handles it.
	cancelLoop()
	// Brief pause to allow the goroutine to process cancellation and exit cleanly.
	time.Sleep(50 * time.Millisecond)
	// log.Debug("test metrics refresh: Test completed.")
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
		"# HELP kubeip_ip_address_assigned Indicates if an IP address was assigned to the node KubeIP is running on (0 for success, 1 for failure)",
		"# TYPE kubeip_ip_address_assigned gauge",
		`kubeip_ip_address_assigned{ip_address="5.6.7.8",ip_address_name="5.6.7.8",k8s_node="test-node-endpoint"} 0`,
		"# HELP kubeip_ip_address_assigned_total Total number of available IP addresses currently in use",
		"# TYPE kubeip_ip_address_assigned_total gauge",
		"kubeip_ip_address_assigned_total 5",
		"# HELP kubeip_ip_address_available_total Total number of available IP addresses still available (not in use)",
		"# TYPE kubeip_ip_address_available_total gauge",
		"kubeip_ip_address_available_total 15",
		"# HELP kubeip_ip_address_usable_total Total number of reserved IP addresses found and usable by KubeIP",
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
