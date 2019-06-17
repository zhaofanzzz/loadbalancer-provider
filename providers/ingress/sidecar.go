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

package ingress

import (
	"net"
	"strings"

	lbapi "github.com/caicloud/clientset/pkg/apis/loadbalance/v1alpha2"
	corenet "github.com/caicloud/loadbalancer-provider/core/pkg/net"
	"github.com/caicloud/loadbalancer-provider/core/pkg/sysctl"
	core "github.com/caicloud/loadbalancer-provider/core/provider"
	"github.com/caicloud/loadbalancer-provider/pkg/version"
	log "github.com/zoumo/logdog"

	utildbus "k8s.io/kubernetes/pkg/util/dbus"
	k8sexec "k8s.io/kubernetes/pkg/util/exec"
	"k8s.io/kubernetes/pkg/util/iptables"
)

const (
	tableRaw      = "raw"
	iptablesChain = "INGRESS-CONTROLLER"
)

var (
	sysctlAdjustments = map[string]string{
		// about time wait connection
		// https://vincent.bernat.im/en/blog/2014-tcp-time-wait-state-linux
		// http://perthcharles.github.io/2015/08/27/timestamp-NAT/
		// deprecated: allow to reuse TIME-WAIT sockets for new connections when it is safe from protocol viewpoint
		// "net.ipv4.tcp_tw_reuse": "1",
		// deprecated: enable fast recycling of TIME-WAIT sockets
		// "net.ipv4.tcp_tw_recycle": "1",

		// about tcp keepalive
		// set tcp keepalive timeout time
		//"net.ipv4.tcp_keepalive_time": "1800",
		// set tcp keepalive probe interval
		//"net.ipv4.tcp_keepalive_intvl": "30",
		// set tcp keepalive probe times
		//"net.ipv4.tcp_keepalive_probes": "3",

		// reduse time wait buckets
		"net.ipv4.tcp_max_tw_buckets": "6000",
		// expand local port range
		//"net.ipv4.ip_local_port_range": "10240 65000",
		// reduse time to hold socket in state FIN-WAIT-2
		//"net.ipv4.tcp_fin_timeout": "30",
		// increase the maximum length of the queue for incomplete sockets
		"net.ipv4.tcp_max_syn_backlog": "8192",

		// increase the queue length for completely established sockets
		"net.core.somaxconn": "2048",
		// expand number of unprocessed input packets before kernel starts dropping them
		"net.core.netdev_max_backlog": "262144",
	}
)
var _ core.Provider = &IngressSidecar{}

// IngressSidecar ...
type IngressSidecar struct {
	nodeInfo      *corenet.Interface
	storeLister   core.StoreLister
	ipt           iptables.Interface
	sysctlDefault map[string]string
	tcpPorts      []string
	udpPorts      []string
}

// NewIngressSidecar creates a new ingress sidecar
func NewIngressSidecar(nodeIP net.IP, lb *lbapi.LoadBalancer) (*IngressSidecar, error) {
	nodeInfo, err := corenet.InterfaceByIP(nodeIP.String())
	if err != nil {
		log.Error("get node info err", log.Fields{"err": err})
		return nil, err
	}
	execer := k8sexec.New()
	dbus := utildbus.New()
	iptInterface := iptables.New(execer, dbus, iptables.ProtocolIpv4)

	sidecar := &IngressSidecar{
		nodeInfo:      nodeInfo,
		sysctlDefault: make(map[string]string),
		ipt:           iptInterface,
	}

	return sidecar, nil
}

// OnUpdate ...
func (p *IngressSidecar) OnUpdate(lb *lbapi.LoadBalancer) error {
	// FIX: issue #3
	// if err := lbapi.ValidateLoadBalancer(lb); err != nil {
	// 	log.Error("invalid loadbalancer", log.Fields{"err": err})
	// 	return nil
	// }

	// // filtered
	// if lb.Spec.Type != lbapi.LoadBalancerTypeExternal || lb.Spec.Providers.Ipvsdr == nil {
	// 	return nil
	// }

	// tcpcm, err := p.storeLister.ConfigMap.ConfigMaps(lb.Namespace).Get(lb.Status.ProxyStatus.TCPConfigMap)
	// if err != nil {
	// 	log.Error("can not find tcp configmap for loadbalancer")
	// 	return err
	// }
	// udpcm, err := p.storeLister.ConfigMap.ConfigMaps(lb.Namespace).Get(lb.Status.ProxyStatus.UDPConfigMap)
	// if err != nil {
	// 	log.Error("can not find udp configmap for loadbalancer")
	// 	return err
	// }

	// tcpPorts, udpPorts := core.GetExportedPorts(tcpcm, udpcm)

	// if reflect.DeepEqual(p.tcpPorts, tcpPorts) && reflect.DeepEqual(p.udpPorts, udpPorts) {
	// 	// no change
	// 	return nil
	// }

	// log.Info("Updating config")

	// p.tcpPorts = tcpPorts
	// p.udpPorts = udpPorts

	// p.ensureIptablesNotrack(tcpPorts, udpPorts)

	return nil
}

// Start ...
func (p *IngressSidecar) Start() {
	log.Info("Startting ingress sidecar provider")

	p.changeSysctl()
	// p.ensureChain()
	return
}

// WaitForStart ...
func (p *IngressSidecar) WaitForStart() bool {
	return true
}

// Stop ...
func (p *IngressSidecar) Stop() error {
	log.Info("Shutting down ingress sidecar provider")

	err := p.resetSysctl()
	if err != nil {
		log.Error("reset sysctl error", log.Fields{"err": err})
	}

	// p.deleteChain()

	return nil
}

// Info ...
func (p *IngressSidecar) Info() core.Info {
	info := version.Get()
	return core.Info{
		Name:      "ingress-sidecar",
		Version:   info.Version,
		GitCommit: info.GitCommit,
		GitRemote: info.GitRemote,
	}
}

// SetListers sets the configured store listers in the generic ingress controller
func (p *IngressSidecar) SetListers(lister core.StoreLister) {
	p.storeLister = lister
}

// changeSysctl changes the required network setting in /proc to get
// keepalived working in the local system.
func (p *IngressSidecar) changeSysctl() error {
	var err error
	p.sysctlDefault, err = sysctl.BulkModify(sysctlAdjustments)
	if err != nil {
		log.Error("error change sysctl", log.Fields{"err": err})
		return err
	}
	return nil
}

// resetSysctl resets the network setting
func (p *IngressSidecar) resetSysctl() error {
	log.Info("reset sysctl to original value", log.Fields{"defaults": p.sysctlDefault})
	_, err := sysctl.BulkModify(p.sysctlDefault)
	return err
}

func (p *IngressSidecar) ensureChain() {
	// create chain
	ae, err := p.ipt.EnsureChain(tableRaw, iptables.Chain(iptablesChain))
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}
	if ae {
		log.Infof("chain %v already existed", iptablesChain)
	}

	// add rule to let all traffic jump to our chain
	p.ipt.EnsureRule(iptables.Append, tableRaw, iptables.ChainPrerouting, "-j", iptablesChain)
}

func (p *IngressSidecar) flushChain() {
	log.Info("flush iptables rules", log.Fields{"table": tableRaw, "chain": iptablesChain})
	p.ipt.FlushChain(tableRaw, iptables.Chain(iptablesChain))
}

func (p *IngressSidecar) deleteChain() {
	// flush chain
	p.flushChain()
	// delete jump rule
	p.ipt.DeleteRule(tableRaw, iptables.ChainPrerouting, "-j", iptablesChain)
	// delete chain
	p.ipt.DeleteChain(tableRaw, iptablesChain)
}

func (p *IngressSidecar) setIptablesNotrack(protocol string, ports []string) (bool, error) {
	args := make([]string, 0)
	args = append(args, "-i", p.nodeInfo.Name, "-p", protocol)

	if len(ports) > 0 {
		args = append(args, "-m", "multiport", "--dports", strings.Join(ports, ","))
	}
	args = append(args, "-j", "NOTRACK")

	return p.ipt.EnsureRule(iptables.Prepend, tableRaw, iptablesChain, args...)
}

func (p *IngressSidecar) ensureIptablesNotrack(tcpPorts, udpPorts []string) {
	log.Info("ensure iptables rules")

	// flush all rules
	p.flushChain()

	if len(tcpPorts) > 0 {
		_, err := p.setIptablesNotrack("tcp", tcpPorts)
		if err != nil {
			log.Error("error ensure iptables tcp rule for", log.Fields{"tcpPorts": tcpPorts})
		}
	}
	if len(udpPorts) > 0 {
		_, err := p.setIptablesNotrack("udp", udpPorts)
		if err != nil {
			log.Error("error ensure iptables udp rule for", log.Fields{"udpPorts": udpPorts})
		}
	}

}
