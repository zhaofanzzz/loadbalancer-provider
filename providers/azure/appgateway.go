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
		listenerSet := make(map[string]struct{})
		if ag.HTTPListeners != nil {
			for _, listener := range *ag.HTTPListeners {
				listenerSet[to.String(listener.Name)] = struct{}{}
			}
		}

		ingInfo := make(map[string]string)
		for _, ing := range ingresses {
			if _, ok := listenerSet[getAGListenerName(ing.Name)]; ok {
				ingInfo[ing.Name] = ing.Spec.Rules[0].Host
			}
		}
		ag = addAllAzureRule(ag, lb, ingInfo)
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
	IDPrefix := strings.SplitAfter(portID, to.String(ag.Name))[0]
	backendID := getAGBackendID(IDPrefix, poolName)
	listenerID := getAGListenerID(IDPrefix, listenerName)
	updated := addAppGatewayRequestRoutingRule(result, ruleName, backendID, listenerID)

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
			if to.Int32(port.Port) == 80 {
				return to.String(port.ID)
			}
		}
	}

	return ""
}

func addAppGatewayHttpListener(ag *network.ApplicationGateway, listenerName, hostname, portID string) *network.ApplicationGateway {
	if ag.HTTPListeners == nil {
		ag.HTTPListeners = &[]network.ApplicationGatewayHTTPListener{}
	}
	*ag.HTTPListeners = append(*ag.HTTPListeners, network.ApplicationGatewayHTTPListener{
		Name: &listenerName,
		ApplicationGatewayHTTPListenerPropertiesFormat: &network.ApplicationGatewayHTTPListenerPropertiesFormat{
			Protocol: "Http",
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

func addAppGatewayRequestRoutingRule(ag *network.ApplicationGateway, ruleName, backendID, listenerID string) *network.ApplicationGateway {
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
				ID: (*ag.ApplicationGatewayPropertiesFormat.BackendHTTPSettingsCollection)[0].ID,
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
		if v == "Success" {
			ruleName := getAGRuleName(k)
			listenerName := getAGListenerName(k)
			result := deleteAppGatewayRequestRoutingRule(ag, ruleName)

			// delete application gateway http listener
			ag = deleteAppGatewayHttpListener(result, listenerName)
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
		IDPrefix := strings.SplitAfter(portID, to.String(ag.Name))[0]
		backendID := getAGBackendID(IDPrefix, poolName)
		listenerID := getAGListenerID(IDPrefix, listenerName)
		ag = addAppGatewayRequestRoutingRule(result, ruleName, backendID, listenerID)
	}
	return ag
}

func deleteAzureRule(c *client.Client, ag *network.ApplicationGateway, groupName, rule string) error {
	// delete application gatway request routing rule
	ruleName := getAGRuleName(rule)
	listenerName := getAGListenerName(rule)
	result := deleteAppGatewayRequestRoutingRule(ag, ruleName)

	// delete application gateway http listener
	updated := deleteAppGatewayHttpListener(result, listenerName)

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

func getAGPoolName(lb string) string {
	return lb + "-backendpool"
}

func getAGRuleName(ing string) string {
	return ing + "-cps-rule"
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
