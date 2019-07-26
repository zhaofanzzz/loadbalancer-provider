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
	"os"
	"syscall"
	"text/template"
	"time"

	"github.com/caicloud/loadbalancer-provider/pkg/execd"
	log "github.com/zoumo/logdog"

	k8sexec "k8s.io/kubernetes/pkg/util/exec"
	"k8s.io/kubernetes/pkg/util/iptables"
)

const (
	iptablesChain  = "LOADBALANCER-IPVS-DR"
	keepalivedCfg  = "/etc/keepalived/keepalived.conf"
	keepalivedTmpl = "/root/keepalived.tmpl"

	acceptMark = 1
	dropMark   = 0
	mask       = "0x00000001"
)

type ipmac struct {
	IP  string
	MAC string
}
type vrrpInstance struct {
	Name      string
	State     string
	Vrid      int
	Priority  int
	Interface string   //TODO
	MyIP      string   //TODO
	AllIPs    []string //TODO
	VIP       string
}

type virtualServer struct {
	AcceptMark int
	VIP        string
	Scheduler  string
	RealServer []string
}

type keepalived struct {
	ipt  iptables.Interface
	cmd  *execd.D
	tmpl *template.Template
	vis  []*vrrpInstance
}

func (k *keepalived) UpdateConfig(vis []*vrrpInstance, vss []*virtualServer) error {
	w, err := os.Create(keepalivedCfg)
	if err != nil {
		return err
	}
	defer w.Close()

	log.Infof("Updating keealived config")
	// save vips for release when shutting down

	k.vis = vis
	conf := make(map[string]interface{})
	conf["iptablesChain"] = iptablesChain
	conf["instances"] = vis
	conf["vss"] = vss

	return k.tmpl.Execute(w, conf)
}

// Start starts a keepalived process in foreground.
// In case of any error it will terminate the execution with a fatal error
func (k *keepalived) Start() {
	ae, err := k.ipt.EnsureChain(iptables.TableFilter, iptables.Chain(iptablesChain))
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}
	if ae {
		log.Infof("chain %v already existed", iptablesChain)
	}

	go k.run()
}

func (k *keepalived) isRunning() bool {
	return k.cmd.IsRunning()
}

func (k *keepalived) run() {
	k.cmd = execd.Daemon("keepalived",
		"--dont-fork",
		"--log-console",
		"--release-vips",
		"--pid", "/keepalived.pid")
	// put keepalived in another process group to prevent it
	// to receive signals meant for the controller
	// k.cmd.SysProcAttr = &syscall.SysProcAttr{
	// 	Setpgid: true,
	// 	Pgid:    0,
	// }
	k.cmd.Stdout = os.Stdout
	k.cmd.Stderr = os.Stderr

	k.cmd.SetGracePeriod(1 * time.Second)

	if err := k.cmd.RunForever(); err != nil {
		panic(fmt.Sprintf("can not run keepalived, %v", err))
	}
}

// Reload sends SIGHUP to keepalived to reload the configuration.
func (k *keepalived) Reload() error {
	log.Info("reloading keepalived")
	err := k.cmd.Signal(syscall.SIGHUP)
	if err == execd.ErrNotRunning {
		log.Warn("keepalived is not running, skip the reload")
		return nil
	}
	if err != nil {
		return fmt.Errorf("error reloading keepalived: %v", err)
	}

	return nil
}

// Stop stop keepalived process
func (k *keepalived) Stop() {
	for _, vi := range k.vis {
		k.removeVIP(vi)
	}

	log.Info("flush iptables chain", log.Fields{"table": iptables.TableFilter, "chain": iptablesChain})
	err := k.ipt.FlushChain(iptables.TableFilter, iptables.Chain(iptablesChain))
	if err != nil {
		log.Errorf("unexpected error flushing iptables chain %v: %v", err, iptablesChain)
	}

	log.Info("stop keepalived process")
	err = k.cmd.Stop()
	if err != nil {
		log.Errorf("error stopping keepalived: %v", err)
	}

}

func (k *keepalived) removeVIP(vi *vrrpInstance) error {
	log.Info("removing configured VIP %v from dev %v", vi.VIP, vi.Interface)
	out, err := k8sexec.New().Command("ip", "addr", "del", vi.VIP+"/32", "dev", vi.Interface).CombinedOutput()
	if err != nil {
		return fmt.Errorf("error reloading keepalived: %v\n%s", err, out)
	}
	return nil
}

func (k *keepalived) loadTemplate() error {
	tmpl, err := template.ParseFiles(keepalivedTmpl)
	if err != nil {
		return err
	}
	k.tmpl = tmpl
	return nil
}
