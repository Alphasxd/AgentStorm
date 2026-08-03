package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	agentstormv1alpha1 "github.com/Alphasxd/AgentStorm/api/v1alpha1"
	agentcontroller "github.com/Alphasxd/AgentStorm/internal/controller"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(agentstormv1alpha1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var probeAddr string
	var leaderElection bool
	var watchNamespace string
	var resultAPIURL string
	var resultWriteTokenSecretName string
	var resultWriteTokenSecretKey string
	var includeSensitiveResults bool
	var enableDistributedLimits bool
	var resultUploadTimeout time.Duration
	var otlpEndpoint string
	var allowRedactedTelemetry bool
	var enableKEDA bool
	var kedaMetricsAPIURL string
	var maxWorkersPerRun int
	var maxInFlightCases int64
	var requireResourceQuota bool
	var kedaPollingInterval int
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "address for the metrics endpoint")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "address for health probes")
	flag.BoolVar(&leaderElection, "leader-elect", false, "enable leader election")
	flag.StringVar(&watchNamespace, "watch-namespace", os.Getenv("WATCH_NAMESPACE"), "namespace to watch; empty watches all namespaces")
	flag.StringVar(&resultAPIURL, "result-api-url", "", "base URL for durable result ingestion; empty disables uploads")
	flag.StringVar(&resultWriteTokenSecretName, "result-write-token-secret-name", "agentstorm-result-auth", "per-run-namespace Secret containing the Result API write token")
	flag.StringVar(&resultWriteTokenSecretKey, "result-write-token-secret-key", "write-token", "Secret data key containing the Result API write token")
	flag.BoolVar(&includeSensitiveResults, "include-sensitive-results", false, "upload case output and full error text")
	flag.BoolVar(&enableDistributedLimits, "enable-distributed-limits", false, "require Workers to acquire Result API backed Provider permits")
	flag.DurationVar(&resultUploadTimeout, "result-upload-timeout", 30*time.Second, "timeout for each Result API request")
	flag.StringVar(&otlpEndpoint, "otel-exporter-otlp-endpoint", "", "OTLP/HTTP base endpoint injected into workers; empty disables tracing")
	flag.BoolVar(&allowRedactedTelemetry, "allow-redacted-telemetry", false, "allow runs to export sanitized prompt, output, tool, and allowlisted metadata content")
	flag.BoolVar(&enableKEDA, "enable-keda", false, "allow durable queued runs to create KEDA ScaledJobs")
	flag.StringVar(&kedaMetricsAPIURL, "keda-metrics-api-url", "", "Result API URL reachable by the KEDA operator")
	flag.IntVar(&maxWorkersPerRun, "max-workers-per-run", 100, "controller safety cap for scheduling.maxWorkers")
	flag.Int64Var(&maxInFlightCases, "max-in-flight-cases", 1000, "controller safety cap for Worker count multiplied by per-Worker concurrency")
	flag.BoolVar(&requireResourceQuota, "require-runner-resource-quota", false, "require each execution namespace to define a ResourceQuota")
	flag.IntVar(&kedaPollingInterval, "keda-polling-interval", 2, "KEDA queue polling interval in seconds")
	zapOptions := zap.Options{Development: true}
	zapOptions.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOptions)))
	if resultUploadTimeout < time.Second || resultUploadTimeout > 300*time.Second || resultUploadTimeout%time.Second != 0 {
		ctrl.Log.Error(fmt.Errorf("result-upload-timeout must be 1-300 whole seconds"), "invalid result sink configuration")
		os.Exit(1)
	}
	resultSink := agentcontroller.ResultSinkConfig{
		URL:                  resultAPIURL,
		WriteTokenSecretName: resultWriteTokenSecretName,
		WriteTokenSecretKey:  resultWriteTokenSecretKey,
		IncludeSensitive:     includeSensitiveResults,
		DistributedLimits:    enableDistributedLimits,
		TimeoutSeconds:       int(resultUploadTimeout.Seconds()),
	}
	if err := resultSink.ValidateAndDefault(); err != nil {
		ctrl.Log.Error(err, "invalid result sink configuration")
		os.Exit(1)
	}
	telemetry := agentcontroller.TelemetryConfig{OTLPEndpoint: otlpEndpoint, AllowRedacted: allowRedactedTelemetry}
	if err := telemetry.ValidateAndDefault(); err != nil {
		ctrl.Log.Error(err, "invalid telemetry configuration")
		os.Exit(1)
	}
	if maxWorkersPerRun < 1 || maxWorkersPerRun > 1000 || kedaPollingInterval < 1 || kedaPollingInterval > 300 {
		ctrl.Log.Error(fmt.Errorf("scheduler integer flags are outside their supported range"), "invalid scheduler configuration")
		os.Exit(1)
	}
	scheduler := agentcontroller.SchedulerConfig{
		EnableKEDA: enableKEDA, MetricsAPIURL: kedaMetricsAPIURL,
		MaxWorkersPerRun: int32(maxWorkersPerRun), MaxInFlightCases: maxInFlightCases,
		RequireResourceQuota: requireResourceQuota, PollingInterval: int32(kedaPollingInterval),
	}
	if err := scheduler.ValidateAndDefault(); err != nil {
		ctrl.Log.Error(err, "invalid scheduler configuration")
		os.Exit(1)
	}

	managerOptions := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElection,
		LeaderElectionID:       "agentstorm-controller.agentstorm.io",
	}
	if watchNamespace != "" {
		managerOptions.Cache.DefaultNamespaces = map[string]cache.Config{watchNamespace: {}}
		managerOptions.LeaderElectionNamespace = watchNamespace
		ctrl.Log.Info("limiting controller cache to one namespace", "namespace", watchNamespace)
	}

	manager, err := ctrl.NewManager(ctrl.GetConfigOrDie(), managerOptions)
	if err != nil {
		ctrl.Log.Error(err, "unable to start manager")
		os.Exit(1)
	}
	if err := (&agentcontroller.AgentTestRunReconciler{
		Client:     manager.GetClient(),
		APIReader:  manager.GetAPIReader(),
		Scheme:     manager.GetScheme(),
		ResultSink: resultSink,
		Telemetry:  telemetry,
		Scheduler:  scheduler,
		Recorder:   manager.GetEventRecorderFor("agentstorm-controller"),
	}).SetupWithManager(manager); err != nil {
		ctrl.Log.Error(err, "unable to create controller")
		os.Exit(1)
	}
	if err := manager.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		ctrl.Log.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := manager.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		ctrl.Log.Error(err, "unable to set up ready check")
		os.Exit(1)
	}
	if err := manager.Start(ctrl.SetupSignalHandler()); err != nil {
		ctrl.Log.Error(err, "manager stopped with an error")
		os.Exit(1)
	}
}
