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
	"sort"
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
	nodeName          string
	reloadRateLimiter flowcontrol.RateLimiter
	keepalived        *keepalived
	ipvsCacheChecker  *ipvsCacheCleaner
	storeLister       core.StoreLister
	sysctlDefault     map[string]string
	ipt               iptables.Interface
	cfgMD5            string
	// TODO: vip should not be a global variable here.
	// But it's not an appropriate to refactor it and the iptables rule
	vip string
}

// NewIpvsdrProvider creates a new ipvs-dr LoadBalancer Provider.
func NewIpvsdrProvider(nodeName string, lb *lbapi.LoadBalancer, unicast bool) (*IpvsdrProvider, error) {

	execer := k8sexec.New()
	dbus := utildbus.New()
	iptInterface := iptables.New(execer, dbus, iptables.ProtocolIpv4)

	ipvs := &IpvsdrProvider{
		nodeName:          nodeName,
		reloadRateLimiter: flowcontrol.NewTokenBucketRateLimiter(10.0, 10),
		vip:               lb.Spec.Providers.Ipvsdr.VIP,
		sysctlDefault:     make(map[string]string, 0),
		ipt:               iptInterface,
	}

	ipvs.keepalived = &keepalived{
		ipt: iptInterface,
	}

	ipvs.ipvsCacheChecker = &ipvsCacheCleaner{
		vip:    lb.Spec.Providers.Ipvsdr.VIP,
		stopCh: make(chan struct{}),
	}

	err := ipvs.keepalived.loadTemplate()
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

	log.Info("IPVS: OnUpdating")

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
	if len(lb.Spec.Nodes.Names) == 0 {
		log.Error("no selected nodes")
		return nil
	}

	_, iface, resolvedNeighbors, err := p.getIPs(lb.Spec.Nodes.Names, lb.Spec.Providers.Ipvsdr.Bind, lbapi.ActiveActiveHA)
	if err != nil {
		return err
	}

	// resolvedNeighbors do not contains self
	p.ensureIptablesMark(resolvedNeighbors, iface, tcpPorts, udpPorts)

	err = p.onUpdateKeepalived(lb)
	if err != nil {
		return err
	}

	return nil
}

func convertToKeepalivedProvider(ipvs *lbapi.IpvsdrProvider) *lbapi.KeepalivedProvider {
	kl := lbapi.KeepalivedProvider{
		VIP:       ipvs.VIP,
		Scheduler: ipvs.Scheduler,
		Bind:      ipvs.Bind,
		HAMode:    lbapi.ActiveActiveHA,
	}
	return &kl
}

func (p *IpvsdrProvider) getIPs(nodes []string, bind *lbapi.KeepalivedBind, haMode lbapi.HAMode) (string, string, []ipmac, error) {
	var err error
	var ipmacs []ipmac
	nodeIPs := p.getNodeNetworks(nodes, bind)
	if len(nodeIPs) == 0 {
		log.Error("Cannot get any valid node IP")
		return "", "", ipmacs, err
	}
	myIP, ok := nodeIPs[p.nodeName]
	if !ok {
		log.Error("Cannot get self node IP")
		return "", "", ipmacs, fmt.Errorf("cannot get self node ip")
	}
	iface, err := corenet.InterfaceByIP(myIP)
	if err != nil {
		log.Errorf("Cannot get interface by ip %s", myIP)
		return "", "", ipmacs, err
	}

	for _, node := range nodes {
		ip, ok := nodeIPs[node]
		if !ok {
			continue
		}
		var mac string
		if node != p.nodeName {
			//  if active-active(dr mode), we should ensure all ip in same l2 network
			if haMode == lbapi.ActiveActiveHA {
				var err error
				mac, err = p.resolveNeighbor(ip, iface.Name)
				if err != nil {
					log.Errorf("Cannot get any valid neighbors MAC for %s on %s", ip, iface.Name)
					continue
				}
			}
		}
		ipmacs = append(ipmacs, ipmac{ip, mac})
	}

	return myIP, iface.Name, ipmacs, nil

}

func (p *IpvsdrProvider) getKeepalivedConfigBlock(nodes []string, kl *lbapi.KeepalivedProvider, priority, vrid int) (*vrrpInstance, *virtualServer, error) {

	myIP, iface, ipmacs, err := p.getIPs(nodes, kl.Bind, kl.HAMode)

	if len(ipmacs) == 0 && len(nodes) > 1 {
		log.Warn("Cannot get any valid neighbors MAC")
	}

	var allIPs []string
	for _, ipmac := range ipmacs {
		allIPs = append(allIPs, ipmac.IP)
	}

	state := "BACKUP"
	if kl.HAMode == lbapi.ActivePassiveHA {
		if nodes[len(nodes)-1] == p.nodeName {
			state = "MASTER"
		}
	}

	name := iface
	name = strings.Replace(name, ".", "_", -1)
	name = strings.Replace(name, "-", "_", -1)
	vi := &vrrpInstance{
		Name:      name,
		State:     state,
		Vrid:      vrid,
		Priority:  priority,
		Interface: iface,
		MyIP:      myIP,
		AllIPs:    allIPs,
		VIP:       kl.VIP,
	}

	var vs *virtualServer
	if kl.HAMode == lbapi.ActiveActiveHA {
		vs = &virtualServer{
			AcceptMark: acceptMark,
			VIP:        kl.VIP,
			Scheduler:  string(kl.Scheduler),
			RealServer: allIPs,
		}
	}
	return vi, vs, err
}

func (p *IpvsdrProvider) onUpdateKeepalived(lb *lbapi.LoadBalancer) error {

	nodes := make([]string, len(lb.Spec.Nodes.Names))
	copy(nodes, lb.Spec.Nodes.Names)
	sort.Strings(nodes)

	prority := getNodePriority(p.nodeName, nodes)
	//TODO check
	vrrid := *lb.Status.ProvidersStatuses.Ipvsdr.Vrid
	ipvs := lb.Spec.Providers.Ipvsdr

	vi, vs, err := p.getKeepalivedConfigBlock(nodes, convertToKeepalivedProvider(ipvs), prority, vrrid)
	if err != nil {
		log.Errorf("Error on getKeepalivedConfigBlock for ipvs provider %v", err)
		return err
	}

	vis := []*vrrpInstance{vi}
	vss := []*virtualServer{vs}

	for _, spec := range ipvs.Slaves {
		if spec.HAMode == lbapi.ActiveActiveHA {
			log.Warnf("skip one slave provider %s because only ActiveActive Mode is supported for now", spec.VIP)
			// if want to support multple ActiveActive providers, acceptMark should be better designed.
			continue
		}

		vrrid = vrrid + 1
		vi, vs, err := p.getKeepalivedConfigBlock(nodes, &spec, prority, vrrid)
		if err != nil {
			continue
		}
		vis = append(vis, vi)
		if vs != nil {
			vss = append(vss, vs)
		}
	}

	err = p.keepalived.UpdateConfig(vis, vss)

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
	p.keepalived.Start()
	p.ipvsCacheChecker.start()
	return
}

// WaitForStart waits for ipvsdr fully run
func (p *IpvsdrProvider) WaitForStart() bool {
	err := wait.Poll(time.Second, 60*time.Second, func() (bool, error) {
		return p.keepalived.isRunning(), nil
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

func (p *IpvsdrProvider) getNodeNetworks(nodes []string, bind *lbapi.KeepalivedBind) map[string]string {
	res := make(map[string]string)

	for _, name := range nodes {
		node, err := p.storeLister.Node.Get(name)
		if err != nil {
			continue
		}

		ann := []string{}
		if bind != nil && bind.NodeIPAnnotation != "" {
			ann = append(ann, bind.NodeIPAnnotation)
		}
		ip, err := nodeutil.GetNodeHostIP(node, ann, ann)
		if err != nil {
			log.Errorf("Error resolve ip of node %v", name)
			continue
		}
		res[name] = ip.String()
	}
	return res
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

func (p *IpvsdrProvider) resolveNeighbor(neighbor string, iface string) (string, error) {

	hwAddr, err := arp.Resolve(iface, neighbor)
	if err != nil {
		log.Errorf("failed to resolve hardware address for %v", neighbor)
		return "", err
	}

	return hwAddr.String(), nil
}

func (p *IpvsdrProvider) appendIptablesMark(protocol, iface string, mark int, mac string, ports []string) (bool, error) {
	return p.setIptablesMark(iptables.Append, protocol, iface, mark, mac, ports)
}

func (p *IpvsdrProvider) prependIptablesMark(protocol, iface string, mark int, mac string, ports []string) (bool, error) {
	return p.setIptablesMark(iptables.Prepend, protocol, iface, mark, mac, ports)
}

func (p *IpvsdrProvider) setIptablesMark(position iptables.RulePosition, protocol, iface string, mark int, mac string, ports []string) (bool, error) {
	if len(ports) == 0 {
		return p.ipt.EnsureRule(position, tableMangle, iptablesChain, p.buildIptablesArgs(protocol, iface, mark, mac, "")...)
	}
	// iptables: too many ports specified
	// multiport accept max ports number may be 15
	for _, port := range ports {
		_, err := p.ipt.EnsureRule(position, tableMangle, iptablesChain, p.buildIptablesArgs(protocol, iface, mark, mac, port)...)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func (p *IpvsdrProvider) buildIptablesArgs(protocol, iface string, mark int, mac string, port string) []string {
	args := make([]string, 0)
	args = append(args, "-i", iface, "-d", p.vip, "-p", protocol)
	if port != "" {
		args = append(args, "-m", "multiport", "--dports", port)
	}
	if mac != "" {
		args = append(args, "-m", "mac", "--mac-source", mac)
	}
	args = append(args, "-j", "MARK", "--set-xmark", fmt.Sprintf("%s/%s", strconv.Itoa(mark), mask))
	log.Warnf("build iptables args %v", args)
	return args
}

func (p *IpvsdrProvider) ensureIptablesMark(neighbors []ipmac, iface string, tcpPorts, udpPorts []string) {
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
		_, err := p.appendIptablesMark("tcp", iface, dropMark, neighbor.MAC, nil)
		if err != nil {
			log.Errorf("failed to ensure iptables tcp rule, iface:%s, ip: %v, mac: %v, mark: %v, err: %v", iface, neighbor.IP, neighbor.MAC, dropMark, err)
		}
		_, err = p.appendIptablesMark("udp", iface, dropMark, neighbor.MAC, nil)
		if err != nil {
			log.Error("failed to ensure iptables udp rule for", log.Fields{"ip": neighbor.IP, "mac": neighbor.MAC, "mark": dropMark, "err": err})
		}
	}

	// this two rules must be prepend before mark 0
	// they mark all matched tcp and udp traffics with 1
	if len(tcpPorts) > 0 {
		_, err := p.prependIptablesMark("tcp", iface, acceptMark, "", tcpPorts)
		if err != nil {
			log.Error("error ensure iptables tcp rule for", log.Fields{"tcpPorts": tcpPorts, "err": err})
		}
	}
	if len(udpPorts) > 0 {
		_, err := p.prependIptablesMark("udp", iface, acceptMark, "", udpPorts)
		if err != nil {
			log.Error("error ensure iptables udp rule for", log.Fields{"udpPorts": udpPorts, "err": err})
		}
	}
}
