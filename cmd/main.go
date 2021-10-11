package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/billyshambrook/vk-lambda/pkg/provider"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	cli "github.com/virtual-kubelet/node-cli"
	logruscli "github.com/virtual-kubelet/node-cli/logrus"
	"github.com/virtual-kubelet/node-cli/opts"
	nprovider "github.com/virtual-kubelet/node-cli/provider"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	logruslogger "github.com/virtual-kubelet/virtual-kubelet/log/logrus"
)

var (
	buildVersion = "N/A"
	buildTime    = "N/A"
	k8sVersion   = "v1.18.4" // Inject this build time by parsing mod.go
)

func main() {
	ctx := cli.ContextWithCancelOnSignal(context.Background())
	logger := logrus.StandardLogger()

	log.L = logruslogger.FromLogrus(logrus.NewEntry(logger))
	logConfig := &logruscli.Config{LogLevel: "info"}

	fmt.Println("hasdl")

	o, err := opts.FromEnv()
	if err != nil {
		panic(err)
	}

	o.Provider = "lambda"
	o.Version = strings.Join([]string{k8sVersion, "vk-lambda", buildVersion}, "-")
	o.NodeName = "lambda"

	node, err := cli.New(
		ctx,
		cli.WithProvider("lambda", func(config nprovider.InitConfig) (nprovider.Provider, error) {
			client, err := newClient(o.KubeConfigPath, o.KubeAPIQPS, o.KubeAPIBurst)
			if err != nil {
				return nil, err
			}
			p, err := provider.NewProvider(config, client)
			if err != nil {
				return nil, err
			}
			return p, nil
		}),
		cli.WithBaseOpts(o),
		cli.WithPersistentFlags(logConfig.FlagSet()),
		cli.WithCLIVersion(buildVersion, buildTime),
		cli.WithKubernetesNodeVersion(k8sVersion),
		cli.WithPersistentPreRunCallback(func() error {
			return logruscli.Configure(logConfig, logger)
		}),
	)

	if err != nil {
		panic(err)
	}

	if err := node.Run(ctx); err != nil {
		panic(err)
	}
}

func newClient(configPath string, qps, burst int32) (*kubernetes.Clientset, error) {
	var config *rest.Config

	// Check if the kubeConfig file exists.
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		// Get the kubeconfig from the filepath.
		config, err = clientcmd.BuildConfigFromFlags("", configPath)
		if err != nil {
			return nil, fmt.Errorf("error building client config: %w", err)
		}
	} else {
		// Set to in-cluster config.
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("error building in cluster config: %w", err)
		}
	}

	if qps != 0 {
		config.QPS = float32(qps)
	}

	if burst != 0 {
		config.Burst = int(burst)
	}

	if masterURI := os.Getenv("MASTER_URI"); masterURI != "" {
		config.Host = masterURI
	}

	return kubernetes.NewForConfig(config)
}
