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
	"strconv"

	lbapi "github.com/caicloud/clientset/pkg/apis/loadbalance/v1alpha2"
	v1 "k8s.io/api/core/v1"
)

var (
	// ReservedTCPPorts represents the reserved tcp ports
	ReservedTCPPorts = []string{"450", "451"}
	// ReservedUDPPorts represents the reserved udp ports
	ReservedUDPPorts = []string{}
)

// GetExportedPorts get exported ports from tcp and udp ConfigMap
func GetExportedPorts(lb *lbapi.LoadBalancer, tcpcm, udpcm *v1.ConfigMap) ([]string, []string) {
	tcpPorts := make([]string, 0)
	udpPorts := make([]string, 0)
	tcpPorts = append(tcpPorts, ReservedTCPPorts...)
	udpPorts = append(udpPorts, ReservedUDPPorts...)

	httpPort := GetHTTPPort(lb)
	httpsPort := GetHTTPSPort(lb)

	tcpPorts = append(tcpPorts, strconv.Itoa(httpPort), strconv.Itoa(httpsPort))

	for port := range tcpcm.Data {
		tcpPorts = append(tcpPorts, port)
	}
	for port := range udpcm.Data {
		udpPorts = append(udpPorts, port)
	}
	return tcpPorts, udpPorts

}

func GetHTTPPort(lb *lbapi.LoadBalancer) int {
	p := 80
	if lb.Spec.Proxy.HTTPPort > 1 && lb.Spec.Proxy.HTTPPort < 65535 {
		p = lb.Spec.Proxy.HTTPPort
	}
	return p
}

func GetHTTPSPort(lb *lbapi.LoadBalancer) int {
	p := 443
	if lb.Spec.Proxy.HTTPSPort > 1 && lb.Spec.Proxy.HTTPSPort < 65535 {
		p = lb.Spec.Proxy.HTTPSPort
	}
	return p
}
