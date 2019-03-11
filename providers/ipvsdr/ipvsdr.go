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

package ipvsdr

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	lbapi "github.com/caicloud/clientset/pkg/apis/loadbalance/v1alpha2"
	nodeutil "github.com/caicloud/clientset/util/node"
	"github.com/caicloud/loadbalancer-provider/core/pkg/arp"
	corenet "github.com/caicloud/loadbalancer-provider/core/pkg/net"
	"github.com/caicloud/loadbalancer-provider/core/pkg/sysctl"
	core "github.com/caicloud/loadbalancer-provider/core/provider"
	"github.com/caicloud/loadbalancer-provider/pkg/version"

	log "github.com/zoumo/logdog"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/flowcontrol"
	utildbus "k8s.io/kubernetes/pkg/util/dbus"
	k8sexec "k8s.io/kubernetes/pkg/util/exec"
	"k8s.io/kubernetes/pkg/util/iptables"
)

const (
	tableMangle = "mangle"
)

var _ core.Provider = &IpvsdrProvider{}

var (
	// sysctl changes required by keepalived
	sysctlAdjustments = map[string]string{
		// allows processes to bind() to non-local IP addresses
		"net.ipv4.ip_nonlocal_bind": "1",
		// enable connection tracking for LVS connections
		"net.ipv4.vs.conntrack": "1",
		// Reply only if the target IP address is local address configured on the incoming interface.
		"net.ipv4.conf.all.arp_ignore": "1",
		// Always use the best local address for ARP requests sent on interface.
		"net.ipv4.conf.all.arp_announce": "2",
		// Reply only if the target IP address is local address configured on the incoming interface.
		"net.ipv4.conf.lo.arp_ignore": "1",
		// Always use the best local address for ARP requests sent on interface.
		"net.ipv4.conf.lo.arp_announce": "2",
	}
)

// IpvsdrProvider ...
type IpvsdrProvider struct {
	nodeInfo          *corenet.Interface
	nodeIP            net.IP
	reloadRateLimiter flowcontrol.RateLimiter
	keepalived        *keepalived
	ipvsCacheChecker  *ipvsCacheCleaner
	storeLister       core.StoreLister
	sysctlDefault     map[string]string
	ipt               iptables.Interface
	cfgMD5            string
	vip               string
	nodeIPLabels      []string
	nodeIPAnnotations []string
}

// NewIpvsdrProvider creates a new ipvs-dr LoadBalancer Provider.
func NewIpvsdrProvider(nodeIP net.IP, lb *lbapi.LoadBalancer, unicast bool, labels, annotations []string) (*IpvsdrProvider, error) {
	nodeInfo, err := corenet.InterfaceByIP(nodeIP.String())
	if err != nil {
		log.Error("get node info err", log.Fields{"err": err})
		return nil, err
	}

	execer := k8sexec.New()
	dbus := utildbus.New()
	iptInterface := iptables.New(execer, dbus, iptables.ProtocolIpv4)

	ipvs := &IpvsdrProvider{
		nodeIP:            nodeIP,
		nodeInfo:          nodeInfo,
		reloadRateLimiter: flowcontrol.NewTokenBucketRateLimiter(10.0, 10),
		vip:               lb.Spec.Providers.Ipvsdr.Vip,
		sysctlDefault:     make(map[string]string, 0),
		ipt:               iptInterface,
		nodeIPLabels:      labels,
		nodeIPAnnotations: annotations,
	}

	// neighbors := getNodeNeighbors(nodeInfo, clusterNodes)
	ipvs.keepalived = &keepalived{
		nodeIP:     nodeIP,
		nodeInfo:   nodeInfo,
		useUnicast: unicast,
		ipt:        iptInterface,
	}

	ipvs.ipvsCacheChecker = &ipvsCacheCleaner{
		vip:    lb.Spec.Providers.Ipvsdr.Vip,
		stopCh: make(chan struct{}),
	}

	err = ipvs.keepalived.loadTemplate()
	if err != nil {
		return nil, err
	}

	return ipvs, nil
}

// OnUpdate ...
func (p *IpvsdrProvider) OnUpdate(lb *lbapi.LoadBalancer) error {
	p.reloadRateLimiter.Accept()

	if err := lbapi.ValidateLoadBalancer(lb); err != nil {
		log.Error("invalid loadbalancer", log.Fields{"err": err})
		return nil
	}

	// filtered
	if lb.Spec.Providers.Ipvsdr == nil {
		return nil
	}

	tcpcm, err := p.storeLister.ConfigMap.ConfigMaps(lb.Namespace).Get(lb.Status.ProxyStatus.TCPConfigMap)
	if err != nil {
		log.Error("can not find tcp configmap for loadbalancer")
		return err
	}
	udpcm, err := p.storeLister.ConfigMap.ConfigMaps(lb.Namespace).Get(lb.Status.ProxyStatus.UDPConfigMap)
	if err != nil {
		log.Error("can not find udp configmap for loadbalancer")
		return err
	}

	tcpPorts, udpPorts := core.GetExportedPorts(tcpcm, udpcm)

	// get selected nodes' ip
	selectedNodes := p.getNodesIP(lb.Spec.Nodes.Names)
	if len(selectedNodes) == 0 {
		log.Error("no selected nodes")
		return nil
	}

	svc := virtualServer{
		VIP:        lb.Spec.Providers.Ipvsdr.Vip,
		Scheduler:  string(lb.Spec.Providers.Ipvsdr.Scheduler),
		RealServer: selectedNodes,
	}

	neighbors := p.resolveNeighbors(getNeighbors(p.nodeIP.String(), selectedNodes))

	err = p.keepalived.UpdateConfig(
		[]virtualServer{svc},
		neighbors,
		getNodePriority(p.nodeIP.String(), selectedNodes),
		*lb.Status.ProvidersStatuses.Ipvsdr.Vrid,
	)
	if err != nil {
		log.Error("error update keealived config", log.Fields{"err": err})
		return err
	}

	p.ensureIptablesMark(neighbors, tcpPorts, udpPorts)

	// check md5
	md5, err := checksum(keepalivedCfg)
	if err == nil && md5 == p.cfgMD5 {
		log.Warn("md5 is not changed", log.Fields{"md5.old": p.cfgMD5, "md5.new": md5})
		return nil
	}

	// p.cfgMD5 = md5
	err = p.keepalived.Reload()
	if err != nil {
		log.Error("reload keepalived error", log.Fields{"err": err})
		return err
	}

	return nil
}

// Start ...
func (p *IpvsdrProvider) Start() {
	log.Info("Startting ipvs dr provider")

	p.changeSysctl()
	p.setLoopbackVIP()
	p.ensureChain()
	go p.keepalived.Start()
	p.ipvsCacheChecker.start()
	return
}

// WaitForStart waits for ipvsdr fully run
func (p *IpvsdrProvider) WaitForStart() bool {
	err := wait.Poll(time.Second, 60*time.Second, func() (bool, error) {
		if p.keepalived.started && p.keepalived.cmd != nil && p.keepalived.cmd.Process != nil {
			return true, nil
		}
		return false, nil
	})

	if err != nil {
		return false
	}
	return true
}

// Stop ...
func (p *IpvsdrProvider) Stop() error {
	log.Info("Shutting down ipvs dr provider")

	err := p.resetSysctl()
	if err != nil {
		log.Error("reset sysctl error", log.Fields{"err": err})
	}

	err = p.removeLoopbackVIP()
	if err != nil {
		log.Error("remove loopback vip error", log.Fields{"err": err})
	}

	p.deleteChain()

	p.ipvsCacheChecker.stop()
	p.keepalived.Stop()

	return nil
}

// Info ...
func (p *IpvsdrProvider) Info() core.Info {
	info := version.Get()
	return core.Info{
		Name:      "ipvsdr",
		Version:   info.Version,
		GitCommit: info.GitCommit,
		GitRemote: info.GitRemote,
	}
}

// SetListers sets the configured store listers in the generic ingress controller
func (p *IpvsdrProvider) SetListers(lister core.StoreLister) {
	p.storeLister = lister
}

func (p *IpvsdrProvider) getNodesIP(names []string) []string {
	ips := make([]string, 0)
	if names == nil {
		return ips
	}

	for _, name := range names {
		node, err := p.storeLister.Node.Get(name)
		if err != nil {
			continue
		}
		ip, err := nodeutil.GetNodeHostIP(node, p.nodeIPLabels, p.nodeIPAnnotations)
		if err != nil {
			continue
		}
		ips = append(ips, ip.String())
	}

	return ips
}

func (p *IpvsdrProvider) ensureChain() {
	// create chain
	ae, err := p.ipt.EnsureChain(tableMangle, iptables.Chain(iptablesChain))
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}
	if ae {
		log.Infof("chain %v already existed", iptablesChain)
	}

	// add rule to let all traffic jump to our chain
	p.ipt.EnsureRule(iptables.Append, tableMangle, iptables.ChainPrerouting, "-j", iptablesChain)
}

func (p *IpvsdrProvider) flushChain() {
	log.Info("flush iptables rules", log.Fields{"table": tableMangle, "chain": iptablesChain})
	p.ipt.FlushChain(tableMangle, iptables.Chain(iptablesChain))
}

func (p *IpvsdrProvider) deleteChain() {
	// flush chain
	p.flushChain()
	// delete jump rule
	p.ipt.DeleteRule(tableMangle, iptables.ChainPrerouting, "-j", iptablesChain)
	// delete chain
	p.ipt.DeleteChain(tableMangle, iptablesChain)
}

// changeSysctl changes the required network setting in /proc to get
// keepalived working in the local system.
func (p *IpvsdrProvider) changeSysctl() error {
	var err error
	p.sysctlDefault, err = sysctl.BulkModify(sysctlAdjustments)
	return err
}

// resetSysctl resets the network setting
func (p *IpvsdrProvider) resetSysctl() error {
	log.Info("reset sysctl to original value", log.Fields{"defaults": p.sysctlDefault})
	_, err := sysctl.BulkModify(p.sysctlDefault)
	return err
}

// setLoopbackVIP sets vip to dev lo
func (p *IpvsdrProvider) setLoopbackVIP() error {

	if p.vip == "" {
		return nil
	}

	lo, err := corenet.InterfaceByLoopback()
	if err != nil {
		return err
	}

	out, err := k8sexec.New().Command("ip", "addr", "add", p.vip+"/32", "dev", lo.Name).CombinedOutput()
	if err != nil {
		return fmt.Errorf("set VIP %s to dev lo error: %v\n%s", p.vip, err, out)
	}
	return nil
}

// removeLoopbackVIP removes vip from dev lo
func (p *IpvsdrProvider) removeLoopbackVIP() error {
	log.Info("remove vip from dev lo", log.Fields{"vip": p.vip})

	if p.vip == "" {
		return nil
	}

	lo, err := corenet.InterfaceByLoopback()
	if err != nil {
		return err
	}

	out, err := k8sexec.New().Command("ip", "addr", "del", p.vip+"/32", "dev", lo.Name).CombinedOutput()
	if err != nil {
		return fmt.Errorf("removing configured VIP from dev lo error: %v\n%s", err, out)
	}
	return nil
}

func (p *IpvsdrProvider) resolveNeighbors(neighbors []string) []ipmac {
	resolvedNeighbors := make([]ipmac, 0)

	for _, neighbor := range neighbors {
		hwAddr, err := arp.Resolve(p.nodeInfo.Name, neighbor)
		if err != nil {
			log.Errorf("failed to resolve hardware address for %v", neighbor)
			continue
		}
		resolvedNeighbors = append(resolvedNeighbors, ipmac{IP: neighbor, MAC: hwAddr})
	}
	return resolvedNeighbors
}

func (p *IpvsdrProvider) appendIptablesMark(protocol string, mark int, mac string, ports []string) (bool, error) {
	return p.setIptablesMark(iptables.Append, protocol, mark, mac, ports)
}

func (p *IpvsdrProvider) prependIptablesMark(protocol string, mark int, mac string, ports []string) (bool, error) {
	return p.setIptablesMark(iptables.Prepend, protocol, mark, mac, ports)
}

func (p *IpvsdrProvider) setIptablesMark(position iptables.RulePosition, protocol string, mark int, mac string, ports []string) (bool, error) {
	args := make([]string, 0)
	args = append(args, "-i", p.nodeInfo.Name, "-d", p.vip, "-p", protocol)

	if len(ports) > 0 {
		args = append(args, "-m", "multiport", "--dports", strings.Join(ports, ","))
	}

	if mac != "" {
		args = append(args, "-m", "mac", "--mac-source", mac)
	}

	args = append(args, "-j", "MARK", "--set-xmark", fmt.Sprintf("%s/%s", strconv.Itoa(mark), mask))

	return p.ipt.EnsureRule(position, tableMangle, iptablesChain, args...)
}

func (p *IpvsdrProvider) ensureIptablesMark(neighbors []ipmac, tcpPorts, udpPorts []string) {
	log.Info("ensure iptables rules")

	// flush all rules
	p.flushChain()

	// Accoding to #19
	// we must add the mark 0 firstly and then prepend mark 1
	// so that

	// all neighbors' rules should be under the basic rules, to override it
	// make sure that all traffics which come from the neighbors will be marked with 0
	// and than lvs will ignore it
	for _, neighbor := range neighbors {
		_, err := p.appendIptablesMark("tcp", dropMark, neighbor.MAC.String(), nil)
		if err != nil {
			log.Error("failed to ensure iptables tcp rule for", log.Fields{"ip": neighbor.IP, "mac": neighbor.MAC.String(), "mark": dropMark, "err": err})
		}
		_, err = p.appendIptablesMark("udp", dropMark, neighbor.MAC.String(), nil)
		if err != nil {
			log.Error("failed to ensure iptables udp rule for", log.Fields{"ip": neighbor.IP, "mac": neighbor.MAC.String(), "mark": dropMark, "err": err})
		}
	}

	// this two rules must be prepend before mark 0
	// they mark all matched tcp and udp traffics with 1
	if len(tcpPorts) > 0 {
		_, err := p.prependIptablesMark("tcp", acceptMark, "", tcpPorts)
		if err != nil {
			log.Error("error ensure iptables tcp rule for", log.Fields{"tcpPorts": tcpPorts})
		}
	}
	if len(udpPorts) > 0 {
		_, err := p.prependIptablesMark("udp", acceptMark, "", udpPorts)
		if err != nil {
			log.Error("error ensure iptables udp rule for", log.Fields{"udpPorts": udpPorts})
		}
	}
}
