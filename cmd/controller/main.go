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
	var resultUploadTimeout time.Duration
	var otlpEndpoint string
	var allowRedactedTelemetry bool
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "address for the metrics endpoint")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "address for health probes")
	flag.BoolVar(&leaderElection, "leader-elect", false, "enable leader election")
	flag.StringVar(&watchNamespace, "watch-namespace", os.Getenv("WATCH_NAMESPACE"), "namespace to watch; empty watches all namespaces")
	flag.StringVar(&resultAPIURL, "result-api-url", "", "base URL for durable result ingestion; empty disables uploads")
	flag.StringVar(&resultWriteTokenSecretName, "result-write-token-secret-name", "agentstorm-result-auth", "per-run-namespace Secret containing the Result API write token")
	flag.StringVar(&resultWriteTokenSecretKey, "result-write-token-secret-key", "write-token", "Secret data key containing the Result API write token")
	flag.BoolVar(&includeSensitiveResults, "include-sensitive-results", false, "upload case output and full error text")
	flag.DurationVar(&resultUploadTimeout, "result-upload-timeout", 30*time.Second, "timeout for each Result API request")
	flag.StringVar(&otlpEndpoint, "otel-exporter-otlp-endpoint", "", "OTLP/HTTP base endpoint injected into workers; empty disables tracing")
	flag.BoolVar(&allowRedactedTelemetry, "allow-redacted-telemetry", false, "allow runs to export sanitized prompt, output, tool, and allowlisted metadata content")
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
