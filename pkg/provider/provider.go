package provider

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/virtual-kubelet/node-cli/provider"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	functionPrefix            = "vk"
	functionRoleARNAnnotation = "lambda.amazonaws.com/role-arn"
)

var (
	errNotImplemented = fmt.Errorf("not implemented by Lambda provider")
)

// Provider implements virtual-kubelet Provider interface for AWS Lambda.
type Provider struct {
	// Kubernetes resources
	client *kubernetes.Clientset

	// Node resources
	internalIP         string
	daemonEndpointPort int32
	startTime          time.Time

	// AWS resources
	svc *lambda.Lambda
}

// NewProvider creates a new AWS Lambda virtual-kubelet Provider.
func NewProvider(config provider.InitConfig, client *kubernetes.Clientset) (*Provider, error) {
	// Initialize aws client session configuration.
	awsConfig := aws.NewConfig()
	awsSession, err := session.NewSessionWithOptions(
		session.Options{
			Config:            *awsConfig,
			SharedConfigState: session.SharedConfigEnable,
		},
	)
	if err != nil {
		return nil, err
	}

	p := &Provider{
		internalIP:         config.InternalIP,
		daemonEndpointPort: config.DaemonPort,
		startTime:          time.Now(),
		svc:                lambda.New(awsSession),
		client:             client,
	}

	return p, nil
}

// CreatePod takes a Kubernetes Pod and deploys it within the provider.
func (p *Provider) CreatePod(ctx context.Context, pod *v1.Pod) error {
	log.G(ctx).Infof("Received CreatePod request for %s/%s.", pod.Namespace, pod.Name)

	// pod.OwnerReferences

	name := buildFunctionName(pod.Namespace, pod.Name)

	req := &lambda.CreateFunctionInput{
		FunctionName: aws.String(name),
		PackageType:  aws.String("Image"),
		Code: &lambda.FunctionCode{
			ImageUri: aws.String(pod.Spec.Containers[0].Image),
		},
		ImageConfig: &lambda.ImageConfig{
			EntryPoint:       aws.StringSlice(pod.Spec.Containers[0].Command),
			Command:          aws.StringSlice(pod.Spec.Containers[0].Args),
			WorkingDirectory: aws.String("/var/task"),
		},
	}

	var roleARN string
	if val, ok := pod.Annotations[functionRoleARNAnnotation]; ok {
		roleARN = val
	} else {
		// TODO: Make default roleARN configurable.
		roleARN = "arn:aws:iam::995144700286:role/pipeline-billy-SplitFunctionRole-1HX39ITFNALOS"
	}
	req.Role = aws.String(roleARN)

	_, err := p.svc.CreateFunctionWithContext(ctx, req)
	if err != nil {
		log.G(ctx).WithError(err).Error("failed to create function")
		return err
	}

	return nil
}

// UpdatePod takes a Kubernetes Pod and updates it within the provider.
func (p *Provider) UpdatePod(ctx context.Context, pod *v1.Pod) error {
	log.G(ctx).Infof("Received UpdatePod request for %s/%s.", pod.Namespace, pod.Name)

	return errNotImplemented
}

// DeletePod takes a Kubernetes Pod and deletes it from the provider. Once a pod is deleted, the provider is
// expected to call the NotifyPods callback with a terminal pod status where all the containers are in a terminal
// state, as well as the pod. DeletePod may be called multiple times for the same pod.
func (p *Provider) DeletePod(ctx context.Context, pod *v1.Pod) error {
	log.G(ctx).Infof("Received DeletePod request for %s/%s.", pod.Namespace, pod.Name)

	name := buildFunctionName(pod.Namespace, pod.Name)

	_, err := p.svc.DeleteFunctionWithContext(ctx, &lambda.DeleteFunctionInput{
		FunctionName: aws.String(name),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case lambda.ErrCodeResourceNotFoundException:
				return nil
			}
		}

		log.G(ctx).WithError(err).Error("failed to delete function")
		return err
	}

	return nil
}

// GetPod retrieves a pod by name from the provider (can be cached).
// The Pod returned is expected to be immutable, and may be accessed
// concurrently outside of the calling goroutine. Therefore it is recommended
// to return a version after DeepCopy.
func (p *Provider) GetPod(ctx context.Context, namespace, name string) (*v1.Pod, error) {
	log.G(ctx).Infof("Received GetPod request for %s/%s.", namespace, name)

	fname := buildFunctionName(namespace, name)

	resp, err := p.svc.GetFunctionWithContext(ctx, &lambda.GetFunctionInput{
		FunctionName: aws.String(fname),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case lambda.ErrCodeResourceNotFoundException:
				return nil, errdefs.NotFoundf("pod %s/%s is not found", namespace, name)
			}
		}

		log.G(ctx).WithError(err).Error("failed to delete function")
		return nil, err
	}

	return &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: v1.PodSpec{
			NodeName: "lambda", // TODO: make configurable
			Containers: []v1.Container{
				{
					Name:    "code",
					Image:   aws.StringValue(resp.Code.ImageUri),
					Command: aws.StringValueSlice(resp.Configuration.ImageConfigResponse.ImageConfig.EntryPoint),
					Args:    aws.StringValueSlice(resp.Configuration.ImageConfigResponse.ImageConfig.Command),
				},
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}, nil
}

// GetPodStatus retrieves the status of a pod by name from the provider.
// The PodStatus returned is expected to be immutable, and may be accessed
// concurrently outside of the calling goroutine. Therefore it is recommended
// to return a version after DeepCopy.
func (p *Provider) GetPodStatus(ctx context.Context, namespace, name string) (*v1.PodStatus, error) {
	log.G(ctx).Infof("Received GetPodStatus request for %s/%s.", namespace, name)

	return &v1.PodStatus{}, nil
}

// GetPods retrieves a list of all pods running on the provider (can be cached).
// The Pods returned are expected to be immutable, and may be accessed
// concurrently outside of the calling goroutine. Therefore it is recommended
// to return a version after DeepCopy.
func (p *Provider) GetPods(ctx context.Context) ([]*v1.Pod, error) {
	log.G(ctx).Info("Received GetPods request")

	return []*v1.Pod{}, nil
}

// GetContainerLogs retrieves the logs of a container by name from the provider.
func (p *Provider) GetContainerLogs(ctx context.Context, namespace, podName, containerName string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	log.G(ctx).Infof("Received GetContainerLogs request for %s/%s/%s.", namespace, podName, containerName)

	return nil, errNotImplemented
}

// RunInContainer executes a command in a container in the pod, copying data
// between in/out/err and the container's stdin/stdout/stderr.
func (p *Provider) RunInContainer(ctx context.Context, namespace, podName, containerName string, cmd []string, attach api.AttachIO) error {
	log.G(ctx).Infof("Received RunInContainer request for %s/%s/%s.", namespace, podName, containerName)

	return errNotImplemented
}

// ConfigureNode enables a provider to configure the node object that
// will be used for Kubernetes.
func (p *Provider) ConfigureNode(ctx context.Context, n *v1.Node) {
	log.G(ctx).Infof("Received ConfigureNode request for %s/%s.", n.Namespace, n.Name)

	// TODO: Make pod capacity configurable.
	// TODO: Calculate CPU and Memory based on pod capacity.
	n.Status.Capacity = v1.ResourceList{
		"cpu":    resource.MustParse("600"),
		"memory": resource.MustParse("1000Gi"),
		"pods":   resource.MustParse("1000"),
	}

	// TODO: Deduct running functions.
	n.Status.Allocatable = v1.ResourceList{
		"cpu":    resource.MustParse("600"),
		"memory": resource.MustParse("1000Gi"),
		"pods":   resource.MustParse("1000"),
	}

	n.Status.Addresses = []v1.NodeAddress{
		{
			Type:    v1.NodeInternalIP,
			Address: p.internalIP,
		},
	}

	n.Status.DaemonEndpoints = v1.NodeDaemonEndpoints{
		KubeletEndpoint: v1.DaemonEndpoint{
			Port: p.daemonEndpointPort,
		},
	}

	n.Status.Conditions = []v1.NodeCondition{
		{
			Type:               "Ready",
			Status:             v1.ConditionTrue,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletReady",
			Message:            "kubelet is ready.",
		},
		{
			Type:               "OutOfDisk",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasSufficientDisk",
			Message:            "kubelet has sufficient disk space available",
		},
		{
			Type:               "MemoryPressure",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasSufficientMemory",
			Message:            "kubelet has sufficient memory available",
		},
		{
			Type:               "DiskPressure",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasNoDiskPressure",
			Message:            "kubelet has no disk pressure",
		},
		{
			Type:               "NetworkUnavailable",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "RouteCreated",
			Message:            "RouteController created a route",
		},
	}

	n.Status.NodeInfo.OperatingSystem = "linux"
	n.Status.NodeInfo.Architecture = "amd64"

	n.ObjectMeta.Labels["alpha.service-controller.kubernetes.io/exclude-balancer"] = "true"
	n.ObjectMeta.Labels["node.kubernetes.io/exclude-from-external-load-balancers"] = "true"
}

func buildFunctionName(namespace, name string) string {
	return fmt.Sprintf("%s_%s_%s", functionPrefix, namespace, name)
}
