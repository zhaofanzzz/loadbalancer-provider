/*
Copyright 2017 Caicloud authors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package provider

import (
	"github.com/caicloud/clientset/kubernetes"
	lblisters "github.com/caicloud/clientset/listers/loadbalance/v1alpha2"
	"github.com/caicloud/clientset/listers/resource/v1beta1"
	lbapi "github.com/caicloud/clientset/pkg/apis/loadbalance/v1alpha2"
	v1listers "k8s.io/client-go/listers/core/v1"
	v1beta1listers "k8s.io/client-go/listers/extensions/v1beta1"
)

const (
	AppGatewayName = "loadbalance.caicloud.io/azureAppGatewayName"
	IngressClass = "kubernetes.io/ingress.class"
)

// Provider holds the methods to handle an Provider backend
type Provider interface {
	// Info returns information about the loadbalancer provider
	Info() Info
	// SetListers allows the access of store listers present in the generic controller
	// This avoid the use of the kubernetes client.
	SetListers(StoreLister)
	// OnUpdate callback invoked when loadbalancer changed
	OnUpdate(*lbapi.LoadBalancer) error
	// Start starts the loadbalancer provider
	Start()
	// WaitForStart waits for provider fully run
	WaitForStart() bool
	// Stop shuts down the loadbalancer provider
	Stop() error
}

// Info returns information about the provider.
// This fields contains information that helps to track issues or to
// map the running loadbalancer provider to source code
type Info struct {
	// Name returns the name of the backend implementation
	Name string `json:"name"`
	// Release returns the running version (semver)
	Version string `json:"version"`
	// Build returns information about the git commit
	GitCommit string `json:"gitCommit"`
	// GitRemote return information about the git remote repository
	GitRemote string `json:"gitRemote"`
}

// StoreLister returns the configured store for loadbalancers, nodes
type StoreLister struct {
	LoadBalancer lblisters.LoadBalancerLister
	Node         v1listers.NodeLister
	ConfigMap    v1listers.ConfigMapLister
	Ingress      v1beta1listers.IngressLister
	Secret       v1listers.SecretLister
	Machine      v1beta1.MachineLister
}

// Configuration contains all the settings required by an LoadBalancer controller
type Configuration struct {
	KubeClient            kubernetes.Interface
	Backend               Provider
	LoadBalancerName      string
	LoadBalancerNamespace string
	TCPConfigMap          string
	UDPConfigMap          string
	IngressClass          string
}
