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

package node

import (
	"fmt"
	"net"

	nodeutil "github.com/caicloud/clientset/util/node"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// GetNodeIPForPod returns the node ip where the pod is located
func GetNodeIPForPod(client kubernetes.Interface, podNamespace, podName string, labels, annotations []string) (net.IP, error) {
	if podName == "" || podNamespace == "" {
		return nil, fmt.Errorf("Please check the manifest (for missing POD_NAME or POD_NAMESPACE env variables)")
	}

	pod, err := client.CoreV1().Pods(podNamespace).Get(podName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("Unable to get pod: %s", err)
	}

	node, err := client.CoreV1().Nodes().Get(pod.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("Unable to get node: %s", err)
	}

	ip, err := nodeutil.GetNodeHostIP(node, labels, annotations)
	if err != nil {
		return nil, err
	}

	return ip, nil
}

// GetNodeNameForPod returns the node ip where the pod is located
func GetNodeNameForPod(client kubernetes.Interface, podNamespace, podName string) (string, error) {
	if podName == "" || podNamespace == "" {
		return "", fmt.Errorf("Please check the manifest (for missing POD_NAME or POD_NAMESPACE env variables)")
	}

	pod, err := client.CoreV1().Pods(podNamespace).Get(podName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	return pod.Spec.NodeName, nil
}
