package client

import (
	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2018-06-01/compute"
	"github.com/Azure/azure-sdk-for-go/services/network/mgmt/2018-01-01/network"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/azure/auth"

	core "github.com/caicloud/loadbalancer-provider/core/provider"
)

// environment is the cloud environment operated in China
var environment = azure.ChinaCloud

var baseURL = environment.ResourceManagerEndpoint

func newClientAuthorizer(clientID, clientSecret, tenantID string) (autorest.Authorizer, error) {
	clientConfig := auth.ClientCredentialsConfig{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TenantID:     tenantID,
		AADEndpoint:  environment.ActiveDirectoryEndpoint,
		Resource:     environment.ResourceManagerEndpoint,
	}
	authorizer, err := clientConfig.Authorizer()
	if err != nil {
		return nil, err
	}
	return authorizer, nil
}

//NewConfig new config
func NewConfig(data map[string][]byte) *Config {
	return &Config{
		SubscriptionID: string(data["subscriptionID"]),
		ClientID:       string(data["clientID"]),
		ClientSecret:   string(data["clientSecret"]),
		TenantID:       string(data["tenantID"]),
	}
}

// NewClientWithConfig returns a Azure Client by config
func NewClientWithConfig(config *Config) (*Client, error) {
	authorizer, err := newClientAuthorizer(config.ClientID, config.ClientSecret, config.TenantID)
	if err != nil {
		return nil, NewServiceError("InvalidAuth", err.Error())
	}

	lbClient := network.NewLoadBalancersClientWithBaseURI(baseURL, config.SubscriptionID)
	lbClient.Authorizer = authorizer

	vmClient := compute.NewVirtualMachinesClientWithBaseURI(baseURL, config.SubscriptionID)
	vmClient.Authorizer = authorizer

	networkInterfaceClient := network.NewInterfacesClientWithBaseURI(baseURL, config.SubscriptionID)
	networkInterfaceClient.Authorizer = authorizer

	securityGroupClient := network.NewSecurityGroupsClientWithBaseURI(baseURL, config.SubscriptionID)
	securityGroupClient.Authorizer = authorizer

	return &Client{
		LoadBalancer: &loadBalancerClientWrapper{
			LoadBalancersClient: lbClient,
		},
		VM: &virtualMachineClientWrapper{
			VirtualMachinesClient: vmClient,
		},
		NetworkInterface: &networkInterfaceClientWrapper{
			InterfacesClient: networkInterfaceClient,
		},
		Config: config,
		SecurityGroup: &securityGroupClientWrapper{
			SecurityGroupsClient: securityGroupClient,
		},
	}, nil
}

// NewClient returns a new Azure Client
func NewClient(storeLister *core.StoreLister) (*Client, error) {
	secret, err := getAPISecret(SecretTypeAzure, storeLister)
	if err != nil {
		return nil, err
	}
	return NewClientWithConfig(NewConfig(secret))
}
