package ipvsdr

import (
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/zoumo/golib/netutil"
	log "github.com/zoumo/logdog"
	"k8s.io/apimachinery/pkg/util/wait"
)

type ipvsCacheCleaner struct {
	vip    string
	saved  string
	stopCh chan struct{}
}

func (ipvs *ipvsCacheCleaner) start() {
	go wait.Until(ipvs.worker, 10*time.Second, ipvs.stopCh)
}

func (ipvs *ipvsCacheCleaner) stop() {
	close(ipvs.stopCh)
}

func (ipvs *ipvsCacheCleaner) worker() {
	vipExists := checkVIPExists(ipvs.vip)
	cacheExists := checkCacheExists()
	if !vipExists && cacheExists {
		// vip doesn't exist but cache exists
		// we should clean the rules and wait for cache expiring
		ipvs.ipvsSaveAndClean()
	} else if vipExists || (!vipExists && !cacheExists) {
		// 1. change to master
		// 2. backup but no cache
		ipvs.ipvsRestore()
	}
	return
}

func (ipvs *ipvsCacheCleaner) ipvsSaveAndClean() error {

	if ipvs.saved != "" {
		return nil
	}

	cmd := exec.Command("ipvsadm", "-Sn")
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Errorf("Error save ipvs rules: %v", string(output))
		return err
	}
	// clean the whole table
	msg, err := exec.Command("ipvsadm", "-C").CombinedOutput()
	if err != nil {
		log.Errorf("Error clean ipvs rules: %v", string(msg))
		return err
	}

	ipvs.saved = string(output)
	log.Info("Waiting for ipvs persistent connection hash table being empty")
	return nil
}

func (ipvs *ipvsCacheCleaner) ipvsRestore() error {
	if ipvs.saved == "" {
		return nil
	}

	msg, err := exec.Command("bash", "-c", `echo "`+ipvs.saved+`"| ipvsadm -R`).CombinedOutput()
	if err != nil {
		log.Errorf("Error restore ipvs rules: %v", string(msg))
		return err
	}

	ipvs.saved = ""
	log.Info("Restore ipvs rules")
	return nil

}

func checkVIPExists(ip string) bool {
	slice, err := netutil.InterfacesByIP(ip)
	if err != nil {
		log.Errorf("Error get net interfaces by ip", ip)
		return false
	}
	for _, iface := range slice {
		if iface.IsLoopback() {
			// skip loopback
			continue
		}
		return true
	}
	return false
}

func checkCacheExists() bool {
	i, err := getLVSCacheLineNumber()
	if err != nil {
		return false
	}
	return i > 0
}

func getLVSCacheLineNumber() (int, error) {
	cmd := exec.Command("bash", "-c", `ipvsadm -Lnc | wc -l`)
	output, err := cmd.CombinedOutput()
	out := string(output)
	if err != nil {
		log.Error(out)
		return 0, err
	}
	i, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		log.Errorf("Error to convert %v to int, err %v", out, err)
		return 0, err
	}

	// the first two lines are headers not entries
	i -= 2
	if i < 0 {
		i = 0
	}
	return i, nil
}
