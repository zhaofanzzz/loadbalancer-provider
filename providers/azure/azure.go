package azure

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/services/network/mgmt/2018-01-01/network"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/to"
	log "github.com/zoumo/logdog"
	v1beta1 "k8s.io/api/extensions/v1beta1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/caicloud/clientset/kubernetes"
	lbapi "github.com/caicloud/clientset/pkg/apis/loadbalance/v1alpha2"
	core "github.com/caicloud/loadbalancer-provider/core/provider"
	"github.com/caicloud/loadbalancer-provider/pkg/version"
	"github.com/caicloud/loadbalancer-provider/providers/azure/client"
)

// AzureProvider azure lb provider
type AzureProvider struct {
	storeLister           core.StoreLister
	clientset             *kubernetes.Clientset
	loadBalancerNamespace string
	loadBalancerName      string

	// old azure lb azure spec
	oldAzureProvider *lbapi.AzureProvider
	nodes            []string
	oldAppGateway    string
	oldResourceGroup string
	ingressesNum     int

	// load balancer rules cache
	tcpRuleMap map[string]string
	udpRuleMap map[string]string

	// cleanAzure
	cleanAzure bool
}

// New creates a new azure LoadBalancer Provider.
func New(clientset *kubernetes.Clientset, name, namespace string) (*AzureProvider, error) {
	azure := &AzureProvider{
		clientset:             clientset,
		loadBalancerName:      name,
		loadBalancerNamespace: namespace,
	}
	return azure, nil
}

// SetListers set store lister
func (l *AzureProvider) SetListers(storeLister core.StoreLister) {
	l.storeLister = storeLister
}

func (l *AzureProvider) setCacheAzureLoadbalancer(azure *lbapi.AzureProvider) {
	if l.oldAzureProvider == nil {
		l.oldAzureProvider = &lbapi.AzureProvider{}
	}
	l.oldAzureProvider.Name = azure.Name
	l.oldAzureProvider.ResourceGroupName = azure.ResourceGroupName
}

func (l *AzureProvider) setCacheReserveStatus(reserve *bool) {
	if l.oldAzureProvider == nil {
		l.oldAzureProvider = &lbapi.AzureProvider{}
	}
	log.Infof("set reserve status %v .", to.Bool(reserve))
	l.oldAzureProvider.ReserveAzure = reserve
}

func (l *AzureProvider) setCacheAzureAppGateway(ag string) {
	l.oldAppGateway = ag
}

func (l *AzureProvider) setCacheAzureResourceGroup(rg string) {
	l.oldResourceGroup = rg
}

func (l *AzureProvider) setCacheAzureingressesNum(len int) {
	l.ingressesNum = len
}

func (l *AzureProvider) setCacheNodes(nodes []string) {
	l.nodes = nodes
}

func hasAzureFinalizer(lb *lbapi.LoadBalancer) bool {
	for _, v := range lb.Finalizers {
		if v == azureFinalizer {
			return true
		}
	}
	return false
}

// OnUpdate update loadbalancer
func (l *AzureProvider) OnUpdate(lb *lbapi.LoadBalancer) error {

	log.Infof("OnUpdate......")

	nlb := lb.DeepCopy()
	if nlb.Annotations == nil {
		nlb.Annotations = make(map[string]string)
	}
	agStatus := lb.Annotations[BackendpoolStatus]
	if agStatus != "" && agStatus != StatusSuccess && agStatus != StatusError &&
		(lb.Annotations[AppGatewayName] != l.oldAppGateway || !reflect.DeepEqual(l.nodes, lb.Spec.Nodes.Names)) {
		l.updateAzureAppGateway(nlb)
	}

	ing, err := l.filterIngress(lb)
	if err != nil {
		return err
	}
	if len(ing) != l.ingressesNum && lb.Annotations[AppGateway] == "true" {
		l.updateIngress(ing, nlb, nil)
	} else if lb.Annotations[AppGateway] == "true" {
		fing := l.filterFinalizerIngress(ing)
		if fing != nil {
			l.updateIngress(ing, nlb, fing)
		}
	}

	if lb.Spec.Providers.Azure == nil {
		return l.cleanupAzureLB(nil, false)
	}

	if lb.DeletionTimestamp != nil {
		return l.cleanupAzureLB(lb, true)
	}

	// ignore change of azure's name groupName and reserve status
	l.setCacheAzureLoadbalancer(lb.Spec.Providers.Azure)
	l.setCacheReserveStatus(lb.Spec.Providers.Azure.ReserveAzure)

	// tell if change of load balancer
	tcp, udp, ruleChange, err := l.getProxyConfigMapAndCompare(lb)
	if err != nil {
		return err
	}
	// ignore change of other providers
	if reflect.DeepEqual(lb.Spec.Providers.Azure, l.oldAzureProvider) &&
		reflect.DeepEqual(l.nodes, lb.Spec.Nodes.Names) &&
		!ruleChange {
		return nil
	}

	// check lb exist in cluster
	_, err = l.storeLister.LoadBalancer.LoadBalancers(lb.Namespace).Get(lb.Name)
	if errors.IsNotFound(err) {
		return l.cleanupAzureLB(lb, true)
	}
	if err != nil {
		log.Errorf("get lb falied %v", err)
		return err
	}

	azlb, ip, err := l.ensureSync(nlb, tcp, udp)

	l.updateLoadBalancerAzureStatus(azlb, lb, ip, err)
	if err == nil {
		log.Infof("update cache data %v tcp %v udp %v", nlb.Spec.Providers.Azure, tcp, udp)
		l.updateCacheData(nlb, tcp, udp, len(ing))
	}
	return err
}

func (l *AzureProvider) updateCacheData(lb *lbapi.LoadBalancer, tcp, udp map[string]string, ing int) {
	l.oldAzureProvider = lb.Spec.Providers.Azure
	l.nodes = lb.Spec.Nodes.Names
	l.tcpRuleMap = tcp
	l.udpRuleMap = udp
	l.oldAppGateway = lb.ObjectMeta.Annotations[AppGatewayName]
	l.oldResourceGroup = lb.ObjectMeta.Annotations[ResourceGroup]
	l.ingressesNum = ing
}

// Start ...
func (l *AzureProvider) Start() {
	log.Infof("Startting azure provider ns %s name %s", l.loadBalancerNamespace, l.loadBalancerName)
	return
}

// Stop ...
func (l *AzureProvider) Stop() error {
	log.Infof("end provider azure...")
	_, err := l.storeLister.LoadBalancer.LoadBalancers(l.loadBalancerNamespace).Get(l.loadBalancerName)
	if errors.IsNotFound(err) {
		return l.cleanupAzureLB(nil, true)
	}
	return nil
}

// WaitForStart waits for
func (l *AzureProvider) WaitForStart() bool {
	// err := wait.Poll(time.Second, 60*time.Second, func() (bool, error) {
	// 	//
	// 	return true, nil
	// })

	// if err != nil {
	// 	return false
	// }
	return true
}

// Info information about the provider.
func (l *AzureProvider) Info() core.Info {
	info := version.Get()
	return core.Info{
		Name:      "azure",
		Version:   info.Version,
		GitCommit: info.GitCommit,
		GitRemote: info.GitRemote,
	}
}

func (l *AzureProvider) updateLoadBalancerAzureStatus(azlb *network.LoadBalancer, lb *lbapi.LoadBalancer, publicIPAddress string, err error) {
	if azlb != nil {
		setProvisioningState(lb, to.String(azlb.ProvisioningState))
	}
	setProvisioningPublicIPAddress(lb, publicIPAddress)
	if err == nil {
		l.patchLoadBalancerAzureStatus(lb, lbapi.AzureRunningPhase, nil)
	} else {
		l.patchLoadBalancerAzureStatus(lb, lbapi.AzureErrorPhase, err)
	}
}

// make sure azure lb config stay in same with compass lb
func (l *AzureProvider) ensureSync(lb *lbapi.LoadBalancer, tcp, udp map[string]string) (*network.LoadBalancer, string, error) {

	azureSpec := lb.Spec.Providers.Azure
	log.Infof("start sync azlb group %s name %s tcp %v", azureSpec.ResourceGroupName, azureSpec.Name, tcp)

	// update status
	_, err := l.patchLoadBalancerAzureStatus(lb, lbapi.AzureUpdatingPhase, nil)
	if err != nil {
		return nil, "", err
	}

	c, err := client.NewClient(&l.storeLister)
	if err != nil {
		log.Errorf("init client error %v", err)
		return nil, "", err
	}

	// get a valid azure load balancer
	azlb, err := l.ensureAzureLoadbalancer(c, lb)
	if err != nil {
		return nil, "", err
	}

	// make sure default config is correct
	azlb, err = ensureSyncDefaultAzureLBConfig(c, azlb, lb)

	if err != nil {
		return nil, "", err
	}

	ip, err := getPublicIPAddress(c, lb)
	if err != nil {
		return nil, "", err
	}

	err = ensureSyncRulesAndBackendPools(c, &l.storeLister, azlb, lb, tcp, udp)

	return azlb, ip, err
}

func getPublicIPAddress(c *client.Client, lb *lbapi.LoadBalancer) (string, error) {
	if lb != nil && lb.Spec.Providers.Azure != nil &&
		lb.Spec.Providers.Azure != nil && lb.Spec.Providers.Azure.IPAddressProperties.Public != nil {
		public := lb.Spec.Providers.Azure.IPAddressProperties.Public
		group, name, err := getGroupAndResourceNameFromID(to.String(public.PublicIPAddressID), azurePublicIPAddresses)
		if err != nil {
			return "", err
		}
		address, err := c.PublicIPAddress.Get(context.TODO(), group, name, "")
		if err != nil {
			return "", err
		}
		return to.String(address.IPAddress), nil
	}
	return "", nil
}

// get compass lb proxy info and compare with cache data
func (l *AzureProvider) getProxyConfigMapAndCompare(lb *lbapi.LoadBalancer) (map[string]string, map[string]string, bool, error) {
	tcpCm, err := l.storeLister.ConfigMap.ConfigMaps(lb.Namespace).Get(lb.Status.ProxyStatus.TCPConfigMap)
	if err != nil {
		log.Errorf("get namespace %s cm %s failed err : %v", lb.Namespace, lb.Status.ProxyStatus.TCPConfigMap, err)
		return nil, nil, false, client.NewServiceError("K8SStore", err.Error())
	}
	udpCm, err := l.storeLister.ConfigMap.ConfigMaps(lb.Namespace).Get(lb.Status.ProxyStatus.UDPConfigMap)
	if err != nil {
		log.Errorf("get namespace %s cm %s failed err : %v", lb.Namespace, lb.Status.ProxyStatus.TCPConfigMap, err)
		return nil, nil, false, client.NewServiceError("K8SStore", err.Error())
	}
	if !reflect.DeepEqual(l.tcpRuleMap, tcpCm.Data) || !reflect.DeepEqual(l.udpRuleMap, udpCm.Data) {
		return tcpCm.Data, udpCm.Data, true, nil
	}
	return tcpCm.Data, udpCm.Data, false, nil
}

// filterIngress filter ingresses in all namespace
func (l *AzureProvider) filterIngress(lb *lbapi.LoadBalancer) ([]*v1beta1.Ingress, error) {
	ingresses, err := l.storeLister.Ingress.Ingresses(v1.NamespaceAll).List(labels.Everything())
	if err != nil {
		log.Errorf("list ingress error %v", err)
		return nil, err
	}

	var ing []*v1beta1.Ingress
	for _, ingress := range ingresses {
		if ingress.ObjectMeta.Annotations[IngressClass] == lb.Status.ProxyStatus.IngressClass {
			ing = append(ing, ingress)
		}
	}

	return ing, nil
}

// filterFinalizerIngress filter finalizer ingress
func (l *AzureProvider) filterFinalizerIngress(ingresses []*v1beta1.Ingress) *v1beta1.Ingress {
	for _, ing := range ingresses {
		if len(ing.Finalizers) != 0 {
			return ing
		}
	}

	return nil
}

// updateIngress update ingress in azure app gateway
func (l *AzureProvider) updateIngress(ingress []*v1beta1.Ingress, lb *lbapi.LoadBalancer, deleteIngress *v1beta1.Ingress) error {
	log.Info("updating ingress")
	c, err := client.NewClient(&l.storeLister)
	if err != nil {
		log.Errorf("init client error %v", err)
		return err
	}

	ag, err := getAzureAppGateway(c, lb.Annotations[ResourceGroup], lb.Annotations[AppGatewayName])
	if err != nil {
		log.Errorf("get application gateway error %v", err)
		return err
	}

	rStatus := make(map[string]string)
	if lb.Annotations[RuleStatus] != "" {
		if err := json.Unmarshal([]byte(strings.Replace(lb.Annotations[RuleStatus], "\\\"", "\"", -1)), &rStatus); err != nil {
			log.Errorf("annotation rule status unmarshal failed %v", err)
			return err
		}
	}
	rMsg := make(map[string]string)
	if lb.Annotations[RuleMsg] != "" {
		if err := json.Unmarshal([]byte(strings.Replace(lb.Annotations[RuleMsg], "\\\"", "\"", -1)), &rMsg); err != nil {
			log.Errorf("annotation rule msg unmarshal failed %v", err)
			return err
		}
	}

	// delete ingress
	if deleteIngress != nil {
		log.Infof("deleting ingress %s in azure", deleteIngress.Name)
		rStatus[deleteIngress.Name] = "Deleting"
		mjson, _ := json.Marshal(rStatus)
		lb.Annotations[RuleStatus] = string(mjson)
		patch := getAnnotationPatch(lb)
		if _, err := l.clientset.LoadbalanceV1alpha2().LoadBalancers(lb.Namespace).Patch(lb.Name, types.MergePatchType, []byte(patch)); err != nil {
			return err
		}

		err := deleteAzureRule(c, ag, lb.ObjectMeta.Annotations[ResourceGroup], deleteIngress.Name)
		if err != nil {
			rStatus[deleteIngress.Name] = StatusError
			mjson, _ := json.Marshal(rStatus)
			lb.Annotations[RuleStatus] = string(mjson)
			rMsg[deleteIngress.Name] = strings.Replace(err.Error(), "\"", "'", -1)
			emjson, _ := json.Marshal(rMsg)
			lb.Annotations[RuleMsg] = string(emjson)

			patch := getAnnotationPatch(lb)
			if _, err := l.clientset.LoadbalanceV1alpha2().LoadBalancers(lb.Namespace).Patch(lb.Name, types.MergePatchType, []byte(patch)); err != nil {
				return err
			}
			return err
		}
		// update ingress finalizer
		deletePatch := l.getIngressFinalizerPatch(deleteIngress)
		if _, err := l.clientset.ExtensionsV1beta1().Ingresses(deleteIngress.Namespace).Patch(deleteIngress.Name, types.MergePatchType, []byte(deletePatch)); err != nil {
			return err
		}

		delete(rStatus, deleteIngress.Name)
		mjson, _ = json.Marshal(rStatus)
		lb.Annotations[RuleStatus] = string(mjson)
		delete(rMsg, deleteIngress.Name)
		emjson, _ := json.Marshal(rMsg)
		lb.Annotations[RuleMsg] = string(emjson)

		patch = getAnnotationPatch(lb)
		if _, err := l.clientset.LoadbalanceV1alpha2().LoadBalancers(lb.Namespace).Patch(lb.Name, types.MergePatchType, []byte(patch)); err != nil {
			return err
		}

		l.setCacheAzureingressesNum(l.ingressesNum - 1)

		return nil
	}

	// add ingress
	ruleSet := make(map[string]struct{})
	if ag.RequestRoutingRules != nil {
		for _, rule := range *ag.RequestRoutingRules {
			if strings.Contains(to.String(rule.Name), "cps") {
				ruleSet[to.String(rule.Name)] = struct{}{}
			}
		}
	}
	for _, ing := range ingress {
		if _, ok := ruleSet[getAGRuleName(ing.Name)]; !ok {
			log.Infof("adding ingress %s in azure", ing.Name)
			rStatus[ing.Name] = "Adding"
			mjson, _ := json.Marshal(rStatus)
			lb.Annotations[RuleStatus] = string(mjson)
			patch := getAnnotationPatch(lb)
			if _, err := l.clientset.LoadbalanceV1alpha2().LoadBalancers(lb.Namespace).Patch(lb.Name, types.MergePatchType, []byte(patch)); err != nil {
				return err
			}

			err = addAzureRule(c, ag, lb.ObjectMeta.Annotations[ResourceGroup], lb.Name, ing.Name, ing.Spec.Rules[0].Host)
			if err != nil {
				rStatus[ing.Name] = StatusError
				mjson, _ := json.Marshal(rStatus)
				lb.Annotations[RuleStatus] = string(mjson)

				rMsg[ing.Name] = strings.Replace(err.Error(), "\"", "'", -1)
				emjson, _ := json.Marshal(rMsg)
				lb.Annotations[RuleMsg] = string(emjson)

				patch := getAnnotationPatch(lb)
				if _, err := l.clientset.LoadbalanceV1alpha2().LoadBalancers(lb.Namespace).Patch(lb.Name, types.MergePatchType, []byte(patch)); err != nil {
					return err
				}

				return err
			}

			rStatus[ing.Name] = StatusSuccess
			rMsg[ing.Name] = ""
			mjson, _ = json.Marshal(rStatus)
			emjson, _ := json.Marshal(rMsg)
			lb.Annotations[RuleStatus] = string(mjson)
			lb.Annotations[RuleMsg] = string(emjson)
			patch = getAnnotationPatch(lb)
			if _, err := l.clientset.LoadbalanceV1alpha2().LoadBalancers(lb.Namespace).Patch(lb.Name, types.MergePatchType, []byte(patch)); err != nil {
				return err
			}

			l.setCacheAzureingressesNum(len(ingress))

			return nil
		}

		log.Infof("ingress %s already exists in azure rule", ing.Name)
	}
	l.setCacheAzureingressesNum(len(ruleSet))

	return nil
}

// get a valid azure load balancer
func (l *AzureProvider) ensureAzureLoadbalancer(c *client.Client, lb *lbapi.LoadBalancer) (*network.LoadBalancer, error) {
	azureSpec := lb.Spec.Providers.Azure
	azlb, err := getAzureLoadbalancer(c, azureSpec.ResourceGroupName, azureSpec.Name)
	if err != nil {
		return nil, err
	}
	if azlb == nil {
		azlb, err = createAzureLoadBalancer(c, lb)
		if err != nil {
			return nil, err
		}
		if len(azureSpec.Name) == 0 {
			azureSpec.Name = to.String(azlb.Name)
			err = l.pathLoadBalancerName(lb, azureSpec.Name)
			if err != nil {
				return nil, err
			}
		}
	}
	return azlb, nil
}

func (l *AzureProvider) pathLoadBalancerName(lb *lbapi.LoadBalancer, name string) error {
	lb.Spec.Providers.Azure.Name = name
	l.setCacheAzureLoadbalancer(lb.Spec.Providers.Azure)
	patch := fmt.Sprintf(`{"spec":{"providers":{"azure":{"name":"%s"}}}}`, name)
	_, err := l.clientset.LoadbalanceV1alpha2().LoadBalancers(lb.Namespace).Patch(lb.Name, types.MergePatchType, []byte(patch))
	if err != nil {
		log.Errorf("patch lb %s failed %v", lb.Name, err)
		return err
	}
	return nil
}

func (l *AzureProvider) getFinalizersPatch(lb *lbapi.LoadBalancer) string {
	if lb == nil {
		return ""
	}
	remainFinalizes := make([]string, 0, len(lb.Finalizers))
	for i := range lb.Finalizers {
		if lb.Finalizers[i] != azureFinalizer {
			remainFinalizes = append(remainFinalizes, fmt.Sprintf("%q", lb.Finalizers[i]))
		}
	}
	if len(remainFinalizes) == len(lb.Finalizers) {
		return ""
	}
	return fmt.Sprintf(`"metadata":{"finalizers":[%s]}`, strings.Join(remainFinalizes, ","))
}

func (l *AzureProvider) getNilAzureStatus() string {
	return `"status":{"ProvidersStatuses":{"azure":{}}}`
}

func (l *AzureProvider) patachFinalizersAndStatus(lb *lbapi.LoadBalancer, deleteLB bool) error {
	patchs := make([]string, 0, 2)
	finalizers := l.getFinalizersPatch(lb)
	if len(finalizers) != 0 {
		patchs = append(patchs, finalizers)
	}
	if !deleteLB {
		patchs = append(patchs, l.getNilAzureStatus())
	}

	if len(patchs) == 0 {
		return nil
	}

	namespace, name := l.loadBalancerNamespace, l.loadBalancerName
	if lb != nil {
		namespace, name = lb.Namespace, lb.Name
	}

	patchJSON := strings.Join(patchs, ",")
	patchJSON = fmt.Sprintf("{%s}", patchJSON)
	_, err := l.clientset.LoadbalanceV1alpha2().LoadBalancers(namespace).Patch(name, types.MergePatchType, []byte(patchJSON))
	if err != nil {
		log.Errorf("patch lb finalizers %s failed %v patch info %s", name, err, patchJSON)
		return err
	}
	return nil
}

func (l *AzureProvider) getIngressFinalizerPatch(ingress *v1beta1.Ingress) string {
	remainFinalizes := make([]string, 0, len(ingress.Finalizers))
	for i := range ingress.Finalizers {
		if ingress.Finalizers[i] != ingressFinalizer {
			remainFinalizes = append(remainFinalizes, fmt.Sprintf("%q", ingress.Finalizers[i]))
		}
	}
	if len(remainFinalizes) == len(ingress.Finalizers) {
		return ""
	}
	return fmt.Sprintf(`{"metadata":{"finalizers":[%s]}}`, strings.Join(remainFinalizes, ","))
}

func getAnnotationPatch(lb *lbapi.LoadBalancer) string {
	remain := make([]string, 0, len(lb.Annotations))
	for k, v := range lb.Annotations {
		remain = append(remain, fmt.Sprintf(`"%v":"%s"`, k, strings.Replace(string(v), "\"", "\\\"", -1)))
	}
	return fmt.Sprintf(`{"metadata":{"annotations":{%s}}}`, strings.Join(remain, ","))
}

// patch load balancer azure status
func (l *AzureProvider) patchLoadBalancerAzureStatus(lb *lbapi.LoadBalancer, phase lbapi.AzureProviderPhase, result error) (*lbapi.LoadBalancer, error) {
	var reason, message string
	var serviceError *client.ServiceError
	switch t := result.(type) {
	case autorest.DetailedError:
		serviceError = client.ParseServiceError(result)
		if serviceError != nil {
			reason = serviceError.Code
			message = serviceError.Message
		}
	case *client.ServiceError:
		reason = t.Code
		message = t.Message
	default:
		if result != nil {
			reason = "Unknown"
			message = result.Error()
		}
	}

	var provisioningState string
	var publicIPAddress string
	if lb != nil && lb.Status.ProvidersStatuses.Azure != nil {
		provisioningState = lb.Status.ProvidersStatuses.Azure.ProvisioningState
		publicIPAddress = to.String(lb.Status.ProvidersStatuses.Azure.PublicIPAddress)
	}
	var patch string
	if result == nil {
		patch = fmt.Sprintf(azureProviderStatusAndPublicIPAddressFormat, phase, reason, message, provisioningState, publicIPAddress)
	} else {
		patch = fmt.Sprintf(azureProviderStatusFormat, phase, reason, message, provisioningState)
	}

	namespace, name := l.loadBalancerNamespace, l.loadBalancerName
	if lb != nil {
		namespace, name = lb.Namespace, lb.Name
	}
	lb, err := l.clientset.LoadbalanceV1alpha2().LoadBalancers(namespace).Patch(name, types.MergePatchType, []byte(patch))
	if err != nil {
		log.Errorf("patch lb %s failed %v", name, err)
		return nil, err
	}
	return lb, nil
}

// clean up azure lb info and make oldAzureProvider nil
func (l *AzureProvider) cleanupAzureLB(lb *lbapi.LoadBalancer, deleteLB bool) error {
	log.Info("start clean up ")
	if l.cleanAzure {
		log.Info("azure loadbalancer is already clean...")
		return nil
	}

	if l.oldAzureProvider == nil || len(l.oldAzureProvider.Name) == 0 {
		log.Errorf("old azure info nil")
		err := l.patachFinalizersAndStatus(lb, deleteLB)
		if err == nil {
			l.cleanAzure = true
		}
		return err
	}
	c, err := client.NewClient(&l.storeLister)
	if err != nil {
		log.Errorf("init client error %v", err)
		return err
	}

	defer func() {
		if err == nil {
			l.oldAzureProvider = nil
		}
	}()

	err = cleanUpSecurityGroup(c, l.oldAzureProvider.ResourceGroupName, l.oldAzureProvider.Name)
	if err != nil {
		return err
	}

	reserve := to.Bool(l.oldAzureProvider.ReserveAzure)
	if lb != nil && lb.Spec.Providers.Azure != nil {
		reserve = to.Bool(lb.Spec.Providers.Azure.ReserveAzure)
	}

	if reserve {
		err = wait.Poll(5*time.Second, 60*time.Second, func() (bool, error) {
			err = recoverDefaultAzureLoadBalancer(c, l.oldAzureProvider.ResourceGroupName, l.oldAzureProvider.Name)
			if err == nil {
				return true, nil
			}
			return false, nil
		})
		if err != nil {
			l.patchLoadBalancerAzureStatus(lb, lbapi.AzureErrorPhase, err)
			return err
		}
		err = l.patachFinalizersAndStatus(lb, deleteLB)
		if err == nil {
			l.cleanAzure = true
		}
		return err
	}
	log.Infof("delete azure lb group %s name %s", l.oldAzureProvider.ResourceGroupName, l.oldAzureProvider.Name)
	err = c.LoadBalancer.Delete(context.TODO(), l.oldAzureProvider.ResourceGroupName, l.oldAzureProvider.Name)
	log.Infof("delete result %v", err)
	if err != nil {
		return err
	}
	err = l.patachFinalizersAndStatus(lb, deleteLB)
	if err == nil {
		l.cleanAzure = true
	}
	return err
}

// updateAzureAppGateway update the azure app gateway
func (l *AzureProvider) updateAzureAppGateway(lb *lbapi.LoadBalancer) error {
	c, err := client.NewClient(&l.storeLister)
	if err != nil {
		log.Errorf("init client error %v", err)
		return err
	}

	if lb.ObjectMeta.Annotations[AppGateway] == "false" && lb.ObjectMeta.Annotations[BackendpoolStatus] == "Deleting" {
		err := l.deleteAzureAppGateway(lb)
		return err
	}

	var nodeip []network.ApplicationGatewayBackendAddress
	for _, node := range lb.Spec.Nodes.Names {
		node, err := l.clientset.CoreV1().Nodes().Get(node, metav1.GetOptions{})
		if err != nil {
			log.Errorf("get node error %v", err)
			return err
		}
		nodeip = append(nodeip, network.ApplicationGatewayBackendAddress{
			IPAddress: &node.Status.Addresses[0].Address,
		})
	}

	if l.oldAppGateway != lb.ObjectMeta.Annotations[AppGatewayName] && lb.ObjectMeta.Annotations[BackendpoolStatus] == "Updating" {
		err := l.deleteAzureAppGateway(lb)
		if err != nil {
			return err
		}
		err = l.addAzureAppGateway(lb, nodeip, true)
		return err
	}

	if !reflect.DeepEqual(l.nodes, lb.Spec.Nodes.Names) && lb.ObjectMeta.Annotations[BackendpoolStatus] == "Updating" {
		err = updateAppGatewayBackendPoolIP(c, nodeip, lb.ObjectMeta.Annotations[ResourceGroup], lb.ObjectMeta.Annotations[AppGatewayName], lb.Name)
		if err != nil {
			lb.Annotations[BackendpoolStatus] = StatusError
			lb.Annotations[ErrorMsg] = strings.Replace(err.Error(), "\"", "'", -1)
			patch := getAnnotationPatch(lb)
			if _, err := l.clientset.LoadbalanceV1alpha2().LoadBalancers(lb.Namespace).Patch(lb.Name, types.MergePatchType, []byte(patch)); err != nil {
				return err
			}

			return err
		}

		lb.Annotations[BackendpoolStatus] = StatusSuccess
		lb.Annotations[ErrorMsg] = ""
		patch := getAnnotationPatch(lb)
		if _, err := l.clientset.LoadbalanceV1alpha2().LoadBalancers(lb.Namespace).Patch(lb.Name, types.MergePatchType, []byte(patch)); err != nil {
			return err
		}

		l.setCacheNodes(lb.Spec.Nodes.Names)
		return nil
	}

	err = l.addAzureAppGateway(lb, nodeip, false)
	return err
}

// deleteAzureAppGateway delete the azure app gateway
func (l *AzureProvider) deleteAzureAppGateway(lb *lbapi.LoadBalancer) error {
	c, err := client.NewClient(&l.storeLister)
	if err != nil {
		log.Errorf("init client error %v", err)
		return err
	}

	rStatus := make(map[string]string)
	if lb.Annotations[RuleStatus] != "" {
		if err := json.Unmarshal([]byte(strings.Replace(lb.Annotations[RuleStatus], "\\\"", "\"", -1)), &rStatus); err != nil {
			log.Errorf("annotation rule status unmarshal failed %v", err)
			return err
		}
	}
	rMsg := make(map[string]string)
	if lb.Annotations[RuleMsg] != "" {
		if err := json.Unmarshal([]byte(strings.Replace(lb.Annotations[RuleMsg], "\\\"", "\"", -1)), &rMsg); err != nil {
			log.Errorf("annotation rule msg unmarshal failed %v", err)
			return err
		}
	}

	err = deleteAppGatewayBackendPool(c, l.oldResourceGroup, l.oldAppGateway, lb.Name, lb.Annotations[RuleStatus])
	if err != nil {
		lb.Annotations[AppGateway] = "true"
		lb.Annotations[AppGatewayName] = l.oldAppGateway
		lb.Annotations[ResourceGroup] = l.oldResourceGroup
		lb.Annotations[BackendpoolStatus] = StatusError
		lb.Annotations[ErrorMsg] = strings.Replace(err.Error(), "\"", "'", -1)

		for k, v := range rStatus {
			if v == StatusSuccess {
				rStatus[k] = StatusError
				rMsg[k] = strings.Replace(err.Error(), "\"", "'", -1)
			}
		}

		mjson, _ := json.Marshal(rStatus)
		lb.Annotations[RuleStatus] = string(mjson)
		emjson, _ := json.Marshal(rMsg)
		lb.Annotations[RuleMsg] = string(emjson)

		patch := getAnnotationPatch(lb)
		if _, err := l.clientset.LoadbalanceV1alpha2().LoadBalancers(lb.Namespace).Patch(lb.Name, types.MergePatchType, []byte(patch)); err != nil {
			return err
		}
		return err
	}

	lb.Annotations[AppGateway] = ""
	lb.Annotations[BackendpoolStatus] = ""
	lb.Annotations[ErrorMsg] = ""
	for k, v := range rStatus {
		if v == StatusSuccess {
			delete(rStatus, k)
			delete(rMsg, k)
		}
	}
	mjson, _ := json.Marshal(rStatus)
	lb.Annotations[RuleStatus] = string(mjson)
	emjson, _ := json.Marshal(rMsg)
	lb.Annotations[RuleMsg] = string(emjson)
	patch := getAnnotationPatch(lb)
	if _, err := l.clientset.LoadbalanceV1alpha2().LoadBalancers(lb.Namespace).Patch(lb.Name, types.MergePatchType, []byte(patch)); err != nil {
		return err
	}

	l.setCacheAzureAppGateway(lb.Annotations[AppGatewayName])
	l.setCacheAzureResourceGroup(lb.Annotations[ResourceGroup])
	l.setCacheNodes(lb.Spec.Nodes.Names)
	l.setCacheAzureingressesNum(len(rStatus))

	return nil
}

// addAzureAppGateway add the azure app gateway
func (l *AzureProvider) addAzureAppGateway(lb *lbapi.LoadBalancer, nodeip []network.ApplicationGatewayBackendAddress, update bool) error {
	c, err := client.NewClient(&l.storeLister)
	if err != nil {
		log.Errorf("init client error %v", err)
		return err
	}

	if update {
		lb.Annotations[BackendpoolStatus] = "Updating"
		lb.Annotations[AppGateway] = "true"
		lb.Annotations[AppGatewayName] = lb.Annotations[AppGatewayName]
		lb.Annotations[ResourceGroup] = lb.Annotations[ResourceGroup]
		patch := getAnnotationPatch(lb)
		if _, err := l.clientset.LoadbalanceV1alpha2().LoadBalancers(lb.Namespace).Patch(lb.Name, types.MergePatchType, []byte(patch)); err != nil {
			return err
		}
	}

	ing, _ := l.filterIngress(lb)
	err = addAppGatewayBackendPool(c, nodeip, lb.ObjectMeta.Annotations[ResourceGroup], lb.ObjectMeta.Annotations[AppGatewayName], lb.Name, ing)

	rStatus := make(map[string]string)
	rMsg := make(map[string]string)

	if err != nil {
		lb.Annotations[BackendpoolStatus] = StatusError
		lb.Annotations[ErrorMsg] = strings.Replace(err.Error(), "\"", "'", -1)
		for _, ingress := range ing {
			rStatus[ingress.Name] = StatusError
			rMsg[ingress.Name] = strings.Replace(err.Error(), "\"", "'", -1)
		}
		mjson, _ := json.Marshal(rStatus)
		lb.Annotations[RuleStatus] = string(mjson)
		emjson, _ := json.Marshal(rMsg)
		lb.Annotations[RuleMsg] = string(emjson)
		patch := getAnnotationPatch(lb)
		if _, err := l.clientset.LoadbalanceV1alpha2().LoadBalancers(lb.Namespace).Patch(lb.Name, types.MergePatchType, []byte(patch)); err != nil {
			return err
		}

		return err
	}

	lb.Annotations[BackendpoolStatus] = StatusSuccess
	lb.Annotations[ErrorMsg] = ""
	for _, ingress := range ing {
		rStatus[ingress.Name] = StatusSuccess
		rMsg[ingress.Name] = ""
	}
	mjson, _ := json.Marshal(rStatus)
	lb.Annotations[RuleStatus] = string(mjson)
	emjson, _ := json.Marshal(rMsg)
	lb.Annotations[RuleMsg] = string(emjson)
	patch := getAnnotationPatch(lb)
	if _, err := l.clientset.LoadbalanceV1alpha2().LoadBalancers(lb.Namespace).Patch(lb.Name, types.MergePatchType, []byte(patch)); err != nil {
		return err
	}

	l.setCacheAzureAppGateway(lb.Annotations[AppGatewayName])
	l.setCacheAzureResourceGroup(lb.Annotations[ResourceGroup])
	l.setCacheNodes(lb.Spec.Nodes.Names)
	l.setCacheAzureingressesNum(len(rStatus))

	return nil
}

func cleanUpSecurityGroup(c *client.Client, groupName, lbName string) error {
	log.Info("start cleanUpSecurityGroup...")
	// get azure lb
	azlb, err := c.LoadBalancer.Get(context.TODO(), groupName, lbName, "")
	// this can be happened to be deleted by others, ignore it
	if client.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	// get networks
	azBackendPoolMap, err := getAzureBackendPoolIP(&azlb)
	detachs := getDiffBetweenNetworkInterfaces(azBackendPoolMap, nil)
	// get the sg to be delete
	deleteSg, _, err := getSuitableSecurityGroup(c, detachs, nil)
	if err != nil {
		return err
	}
	// delete the useless rules from security group
	return deleteUselessSecurityRules(c, deleteSg)
}
