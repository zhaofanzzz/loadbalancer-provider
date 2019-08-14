package azure

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Azure/azure-sdk-for-go/services/network/mgmt/2018-01-01/network"
	"github.com/Azure/go-autorest/autorest/to"
	"github.com/caicloud/loadbalancer-provider/providers/azure/client"
	log "github.com/zoumo/logdog"
	v1beta1 "k8s.io/api/extensions/v1beta1"
)

const (
	AppGateway        = "loadbalance.caicloud.io/azureAppGateway"
	AppGatewayName    = "loadbalance.caicloud.io/azureAppGatewayName"
	BackendpoolStatus = "loadbalance.caicloud.io/azureBackendPoolStatus"
	RuleStatus        = "loadbalance.caicloud.io/azureRuleStatus"
	RuleMsg           = "loadbalance.caicloud.io/azureRuleErrorMsg"
	ErrorMsg          = "loadbalance.caicloud.io/azureErrorMsg"
	ResourceGroup     = "loadbalance.caicloud.io/azureResourceGroup"
	IngressClass      = "kubernetes.io/ingress.class"
	CompassProbes     = "compass-healthz-probe"
	StatusSuccess     = "Success"
	StatusError       = "Error"
	AzureFrontendPort = 80
	AzureTimeout      = 30
	AzureInterval     = 30
	AzureHealthCheck  = 3
	AzureProbePath    = "/healthz"
	AzureProbeHost    = "127.0.0.1"
	HTTPProtocol      = "Http"
)

func getAzureAppGateway(c *client.Client, groupName, appGatewayName string) (*network.ApplicationGateway, error) {
	if len(appGatewayName) == 0 {
		return nil, errors.New("application gateway name can not be empty")
	}
	ag, err := c.AppGateway.Get(context.TODO(), groupName, appGatewayName)
	if err != nil {
		return nil, err
	}

	return &ag, nil
}

func addAppGatewayBackendPool(c *client.Client, nodeip []network.ApplicationGatewayBackendAddress, groupName, agName, lb string, ingresses []*v1beta1.Ingress) error {
	ag, err := getAzureAppGateway(c, groupName, agName)
	if err != nil {
		log.Errorf("get application %s gateway error %v", agName, err)
		return err
	}
	ag = ensureAppGatewayProbes(ag)

	poolName := getAGPoolName(lb)
	if ag.BackendAddressPools == nil {
		ag.BackendAddressPools = &[]network.ApplicationGatewayBackendAddressPool{}
	}
	*ag.BackendAddressPools = append(*ag.BackendAddressPools, network.ApplicationGatewayBackendAddressPool{
		Name: &poolName,
		ApplicationGatewayBackendAddressPoolPropertiesFormat: &network.ApplicationGatewayBackendAddressPoolPropertiesFormat{
			BackendAddresses: &nodeip,
		},
	})

	if ingresses != nil {
		ruleSet := make(map[string]struct{})
		if ag.RequestRoutingRules != nil {
			for _, rule := range *ag.RequestRoutingRules {
				ruleSet[to.String(rule.Name)] = struct{}{}
			}
		}

		ingInfo := make(map[string]string)
		for _, ing := range ingresses {
			if _, ok := ruleSet[getAGRuleName(ing.Name)]; !ok {
				ingInfo[ing.Name] = ing.Spec.Rules[0].Host
			}
		}
		ag = addAllAzureRule(ag, poolName, ingInfo)
	}

	_, err = c.AppGateway.CreateOrUpdate(context.TODO(), groupName, agName, *ag)
	if err != nil {
		log.Errorf("add app gateway update application gateway error %v", err)
		return err
	}

	return nil
}

func deleteAppGatewayBackendPool(c *client.Client, groupName, agName, lb, rule string) error {
	ag, err := getAzureAppGateway(c, groupName, agName)
	if err != nil {
		log.Errorf("get application gateway error %v", err)
		return err
	}

	poolName := getAGPoolName(lb)
	var bp []network.ApplicationGatewayBackendAddressPool
	if ag.BackendAddressPools != nil {
		for _, pool := range *ag.BackendAddressPools {
			if to.String(pool.Name) != poolName {
				bp = append(bp, pool)
			}
		}
	}

	ag.BackendAddressPools = &bp

	if rule != "" {
		log.Info("deleting all azure rule")
		ruleStatus := strings.Replace(string(rule), "'", "\"", -1)
		rStatus := make(map[string]string)
		if ruleStatus != "" {
			if err := json.Unmarshal([]byte(ruleStatus), &rStatus); err != nil {
				log.Errorf("annotation rule status unmarshal failed %v", err)
				return err
			}
		}
		ag = deleteAllAzureRule(ag, groupName, rStatus)
	}
	_, err = c.AppGateway.CreateOrUpdate(context.TODO(), groupName, agName, *ag)
	if err != nil {
		log.Errorf("delete app gateway update application gateway error %v", err)
		return err
	}

	return nil
}

func updateAppGatewayBackendPoolIP(c *client.Client, nodeip []network.ApplicationGatewayBackendAddress, groupName, agName, lb string) error {
	ag, err := getAzureAppGateway(c, groupName, agName)
	if err != nil || ag == nil {
		log.Errorf("get application gateway error %v", err)
		return err
	}

	poolName := getAGPoolName(lb)
	if ag.BackendAddressPools != nil {
		for index, pool := range *ag.BackendAddressPools {
			if to.String(pool.Name) == poolName {
				(*ag.BackendAddressPools)[index].BackendAddresses = &nodeip
			}
		}
	}

	_, err = c.AppGateway.CreateOrUpdate(context.TODO(), groupName, agName, *ag)
	if err != nil {
		log.Errorf("update backend pool ip update application gateway error %v", err)
		return err
	}

	return nil
}

func addAzureRule(c *client.Client, ag *network.ApplicationGateway, groupName, lbName, rule, hostname string) error {
	// add application gateway http listener
	listenerName := getAGListenerName(rule)
	portID := getFrontendPortID(ag)
	result := addAppGatewayHttpListener(ag, listenerName, hostname, portID)

	// add application gatway request routing rule
	ruleName := getAGRuleName(rule)
	poolName := getAGPoolName(lbName)
	settingName := getAGSettingName(rule)
	IDPrefix := strings.SplitAfter(portID, to.String(ag.Name))[0]
	backendID := getAGBackendID(IDPrefix, poolName)
	listenerID := getAGListenerID(IDPrefix, listenerName)
	settingID := getAGSettingID(IDPrefix, settingName)
	probeID := getAGProbeID(IDPrefix)
	backendSetting := addAppGatewayBackendHTTPSettings(result, settingName, probeID, AzureFrontendPort, AzureTimeout)
	updated := addAppGatewayRequestRoutingRule(backendSetting, ruleName, backendID, listenerID, settingID)

	_, err := c.AppGateway.CreateOrUpdate(context.TODO(), groupName, to.String(ag.Name), *updated)
	if err != nil {
		log.Errorf("add azure rule update application gateway error %v", err)
		return err
	}

	return nil
}

func getFrontendPortID(ag *network.ApplicationGateway) string {
	if ag.FrontendPorts != nil {
		for _, port := range *ag.FrontendPorts {
			if to.Int32(port.Port) == AzureFrontendPort {
				return to.String(port.ID)
			}
		}
	}

	return ""
}

func getProbeID(ag *network.ApplicationGateway) string {
	if ag.Probes != nil {
		for _, probe := range *ag.Probes {
			if to.String(probe.Name) == CompassProbes {
				return to.String(probe.ID)
			}
		}
	}

	return ""
}

func ensureAppGatewayProbes(ag *network.ApplicationGateway) *network.ApplicationGateway {
	for _, probe := range *ag.Probes {
		if to.String(probe.Name) == CompassProbes {
			return ag
		}
	}
	return createAppGatewayProbes(ag, CompassProbes, AzureProbePath, AzureProbeHost, AzureInterval, AzureTimeout, AzureHealthCheck)
}

func createAppGatewayProbes(ag *network.ApplicationGateway, probeName, healthPath, host string, interval, timeout, unhealthy int32) *network.ApplicationGateway {
	if ag.Probes == nil {
		ag.Probes = &[]network.ApplicationGatewayProbe{}
	}
	*ag.Probes = append(*ag.Probes, network.ApplicationGatewayProbe{
		Name: &probeName,
		ApplicationGatewayProbePropertiesFormat: &network.ApplicationGatewayProbePropertiesFormat{
			Path:               &healthPath,
			Protocol:           HTTPProtocol,
			Host:               &host,
			Interval:           &interval,
			Timeout:            &timeout,
			UnhealthyThreshold: &unhealthy,
		},
	})

	return ag
}

func addAppGatewayHttpListener(ag *network.ApplicationGateway, listenerName, hostname, portID string) *network.ApplicationGateway {
	if ag.HTTPListeners == nil {
		ag.HTTPListeners = &[]network.ApplicationGatewayHTTPListener{}
	}
	*ag.HTTPListeners = append(*ag.HTTPListeners, network.ApplicationGatewayHTTPListener{
		Name: &listenerName,
		ApplicationGatewayHTTPListenerPropertiesFormat: &network.ApplicationGatewayHTTPListenerPropertiesFormat{
			Protocol: HTTPProtocol,
			HostName: &hostname,
			FrontendIPConfiguration: &network.SubResource{
				ID: (*ag.ApplicationGatewayPropertiesFormat.FrontendIPConfigurations)[0].ID,
			},
			FrontendPort: &network.SubResource{
				ID: &portID,
			},
		},
	})

	return ag
}

func addAppGatewayBackendHTTPSettings(ag *network.ApplicationGateway, settingName, probeID string, port, timeout int32) *network.ApplicationGateway {
	if ag.BackendHTTPSettingsCollection == nil {
		ag.BackendHTTPSettingsCollection = &[]network.ApplicationGatewayBackendHTTPSettings{}
	}
	*ag.BackendHTTPSettingsCollection = append(*ag.BackendHTTPSettingsCollection, network.ApplicationGatewayBackendHTTPSettings{
		Name: &settingName,
		ApplicationGatewayBackendHTTPSettingsPropertiesFormat: &network.ApplicationGatewayBackendHTTPSettingsPropertiesFormat{
			Port:                &port,
			Protocol:            HTTPProtocol,
			CookieBasedAffinity: "Disabled",
			RequestTimeout:      &timeout,
			Probe: &network.SubResource{
				ID: &probeID,
			},
		},
	})

	return ag
}

func addAppGatewayRequestRoutingRule(ag *network.ApplicationGateway, ruleName, backendID, listenerID, settingID string) *network.ApplicationGateway {
	if ag.RequestRoutingRules == nil {
		ag.RequestRoutingRules = &[]network.ApplicationGatewayRequestRoutingRule{}
	}
	*ag.RequestRoutingRules = append(*ag.RequestRoutingRules, network.ApplicationGatewayRequestRoutingRule{
		Name: &ruleName,
		ApplicationGatewayRequestRoutingRulePropertiesFormat: &network.ApplicationGatewayRequestRoutingRulePropertiesFormat{
			RuleType: "Basic",
			BackendAddressPool: &network.SubResource{
				ID: &backendID,
			},
			BackendHTTPSettings: &network.SubResource{
				ID: &settingID,
			},
			HTTPListener: &network.SubResource{
				ID: &listenerID,
			},
		},
	})

	return ag
}

func deleteAllAzureRule(ag *network.ApplicationGateway, groupName string, rule map[string]string) *network.ApplicationGateway {
	for k, v := range rule {
		if v == StatusSuccess {
			ruleName := getAGRuleName(k)
			listenerName := getAGListenerName(k)
			settingName := getAGSettingName(k)
			result := deleteAppGatewayRequestRoutingRule(ag, ruleName)
			backendSetting := deleteAppGatewayBackendHTTPSettings(result, settingName)

			// delete application gateway http listener
			ag = deleteAppGatewayHttpListener(backendSetting, listenerName)
		}
	}
	return ag
}

func addAllAzureRule(ag *network.ApplicationGateway, poolName string, rule map[string]string) *network.ApplicationGateway {
	for k, v := range rule {
		listenerName := getAGListenerName(k)
		portID := getFrontendPortID(ag)
		result := addAppGatewayHttpListener(ag, listenerName, v, portID)

		// add application gatway request routing rule
		ruleName := getAGRuleName(k)
		settingName := getAGSettingName(k)
		probeID := getProbeID(ag)
		backendSetting := addAppGatewayBackendHTTPSettings(result, settingName, probeID, 80, 30)

		IDPrefix := strings.SplitAfter(portID, to.String(ag.Name))[0]
		backendID := getAGBackendID(IDPrefix, poolName)
		listenerID := getAGListenerID(IDPrefix, listenerName)
		settingID := getAGSettingID(IDPrefix, settingName)
		ag = addAppGatewayRequestRoutingRule(backendSetting, ruleName, backendID, listenerID, settingID)
	}
	return ag
}

func deleteAzureRule(c *client.Client, ag *network.ApplicationGateway, groupName, rule string) error {
	// delete application gatway request routing rule
	ruleName := getAGRuleName(rule)
	listenerName := getAGListenerName(rule)
	settingName := getAGSettingName(rule)
	result := deleteAppGatewayRequestRoutingRule(ag, ruleName)
	backendSetting := deleteAppGatewayBackendHTTPSettings(result, settingName)

	// delete application gateway http listener
	updated := deleteAppGatewayHttpListener(backendSetting, listenerName)

	_, err := c.AppGateway.CreateOrUpdate(context.TODO(), groupName, to.String(updated.Name), *updated)
	if err != nil {
		log.Errorf("update application gateway error %v", err)
		return err
	}

	return nil
}

func deleteAppGatewayHttpListener(ag *network.ApplicationGateway, listenerName string) *network.ApplicationGateway {
	var aghl []network.ApplicationGatewayHTTPListener
	if ag.HTTPListeners != nil {
		for _, listener := range *ag.HTTPListeners {
			if to.String(listener.Name) != listenerName {
				aghl = append(aghl, listener)
			}
		}
	}
	ag.HTTPListeners = &aghl

	return ag
}

func deleteAppGatewayRequestRoutingRule(ag *network.ApplicationGateway, ruleName string) *network.ApplicationGateway {
	var agrr []network.ApplicationGatewayRequestRoutingRule
	if ag.RequestRoutingRules != nil {
		for _, rule := range *ag.RequestRoutingRules {
			if to.String(rule.Name) != ruleName {
				agrr = append(agrr, rule)
			}
		}
	}
	ag.RequestRoutingRules = &agrr

	return ag
}

func deleteAppGatewayBackendHTTPSettings(ag *network.ApplicationGateway, settingName string) *network.ApplicationGateway {
	var agbs []network.ApplicationGatewayBackendHTTPSettings
	if ag.BackendHTTPSettingsCollection != nil {
		for _, setting := range *ag.BackendHTTPSettingsCollection {
			if to.String(setting.Name) != settingName {
				agbs = append(agbs, setting)
			}
		}
	}
	ag.BackendHTTPSettingsCollection = &agbs

	return ag
}

func getAGPoolName(lb string) string {
	return lb + "-backendpool"
}

func getAGRuleName(ing string) string {
	return ing + "-cps-rule"
}

func getAGSettingName(ing string) string {
	return ing + "-cps-http-setting"
}

func getAGListenerName(ing string) string {
	return ing + "-cps-listener"
}

func getAGBackendID(prefix, poolName string) string {
	return prefix + "/backendAddressPools/" + poolName
}

func getAGListenerID(prefix, listenerName string) string {
	return prefix + "/httpListeners/" + listenerName
}

func getAGSettingID(prefix, settingName string) string {
	return prefix + "/backendHttpSettingsCollection/" + settingName
}

func getAGProbeID(prefix string) string {
	return prefix + "/probes/" + CompassProbes
}
