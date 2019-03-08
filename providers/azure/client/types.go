package client

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2018-06-01/compute"
	"github.com/Azure/azure-sdk-for-go/services/network/mgmt/2018-01-01/network"
)

// Config defines the config data required by Azure
type Config struct {
	SubscriptionID string `json:"subscriptionID"`
	ClientID       string `json:"clientID"`
	ClientSecret   string `json:"clientSecret"`
	TenantID       string `json:"tenantID"`
}

// Client defines the Azure clients we need
type Client struct {
	Config           *Config
	LoadBalancer     loadBalancerClient
	VM               virtualMachineClient
	NetworkInterface networkInterfaceClient
	SecurityGroup    securityGroupClient
}

type securityGroupClient interface {
	Get(ctx context.Context, resourceGroupName, loadBalancerName, expand string) (network.SecurityGroup, error)
	CreateOrUpdate(ctx context.Context, resourceGroupName, securityGroupName string, parameters network.SecurityGroup) (network.SecurityGroup, error)
}

type securityGroupClientWrapper struct {
	network.SecurityGroupsClient
}

func (c *securityGroupClientWrapper) CreateOrUpdate(ctx context.Context, resourceGroupName string, securityGroupName string, sg network.SecurityGroup) (network.SecurityGroup, error) {
	createFuture, err := c.SecurityGroupsClient.CreateOrUpdate(ctx, resourceGroupName, securityGroupName, sg)
	if err != nil {
		return network.SecurityGroup{}, err
	}

	err = createFuture.WaitForCompletion(ctx, c.SecurityGroupsClient.Client)
	if err != nil {
		return network.SecurityGroup{}, err
	}

	sg, err = createFuture.Result(c.SecurityGroupsClient)
	return sg, err
}

type loadBalancerClient interface {
	Get(ctx context.Context, resourceGroupName, loadBalancerName, expand string) (network.LoadBalancer, error)
	CreateOrUpdate(ctx context.Context, resourceGroupName, loadBalancerName string, parameters network.LoadBalancer) (network.LoadBalancer, error)
	Delete(ctx context.Context, resourceGroupName, loadBalancerName string) error
	ListAll(ctx context.Context) ([]network.LoadBalancer, error)
}

type loadBalancerClientWrapper struct {
	network.LoadBalancersClient
}

func (l *loadBalancerClientWrapper) ListAll(ctx context.Context) ([]network.LoadBalancer, error) {
	listPage, err := l.LoadBalancersClient.ListAll(ctx)
	if err != nil {
		return nil, err
	}

	var re []network.LoadBalancer
	for listPage.NotDone() {
		lbs := listPage.Values()
		if 0 != len(lbs) {
			re = append(re, lbs...)
		}

		err = listPage.Next()
		if err != nil {
			return nil, err
		}
	}

	return re, nil
}

func (l *loadBalancerClientWrapper) CreateOrUpdate(ctx context.Context,
	resourceGroupName, loadBalancerName string, parameters network.LoadBalancer) (network.LoadBalancer, error) {
	future, e := l.LoadBalancersClient.CreateOrUpdate(ctx, resourceGroupName, loadBalancerName, parameters)
	if e != nil {
		return network.LoadBalancer{}, e
	}
	e = future.WaitForCompletion(ctx, l.LoadBalancersClient.Client)
	if e != nil {
		return network.LoadBalancer{}, e
	}
	return future.Result(l.LoadBalancersClient)
}

func (l *loadBalancerClientWrapper) Delete(ctx context.Context, resourceGroupName, loadBalancerName string) error {
	_, err := l.LoadBalancersClient.Delete(ctx, resourceGroupName, loadBalancerName)
	if err != nil {
		if IsNotFound(err) {
			return nil
		}
		return err
	}

	// err = deleteFuture.WaitForCompletion(ctx, l.LoadBalancersClient.Client)
	// if err != nil {
	// 	return err
	// }
	return nil
}

type virtualMachineClient interface {
	Get(ctx context.Context, resourceGroupName string, VMName string, expand compute.InstanceViewTypes) (compute.VirtualMachine, error)
	ListAll(ctx context.Context) ([]compute.VirtualMachine, error)
	CreateOrUpdate(ctx context.Context, resourceGroupName string, vmName string, vm compute.VirtualMachine) (compute.VirtualMachine, error)
	Delete(ctx context.Context, resourceGroupName string, vmName string) error
}

type virtualMachineClientWrapper struct {
	compute.VirtualMachinesClient
}

func (c *virtualMachineClientWrapper) ListAll(ctx context.Context) ([]compute.VirtualMachine, error) {
	listPage, err := c.VirtualMachinesClient.ListAll(ctx)
	if err != nil {
		return nil, err
	}

	r := make([]compute.VirtualMachine, 0)
	for listPage.NotDone() {
		vms := listPage.Values()
		if 0 != len(vms) {
			r = append(r, vms...)
		}

		err = listPage.Next()
		if err != nil {
			return nil, err
		}
	}

	return r, nil
}

func (c *virtualMachineClientWrapper) CreateOrUpdate(ctx context.Context, resourceGroupName string, vmName string, vm compute.VirtualMachine) (compute.VirtualMachine, error) {
	createFuture, err := c.VirtualMachinesClient.CreateOrUpdate(ctx, resourceGroupName, vmName, vm)
	if err != nil {
		return compute.VirtualMachine{}, err
	}

	err = createFuture.WaitForCompletion(ctx, c.VirtualMachinesClient.Client)
	if err != nil {
		return compute.VirtualMachine{}, err
	}

	vm, err = createFuture.Result(c.VirtualMachinesClient)
	return vm, err
}

func (c *virtualMachineClientWrapper) Delete(ctx context.Context, resourceGroupName string, vmName string) error {
	deleteFuture, err := c.VirtualMachinesClient.Delete(ctx, resourceGroupName, vmName)
	if err != nil {
		if IsNotFound(err) {
			return nil
		}
		return err
	}

	err = deleteFuture.WaitForCompletion(ctx, c.VirtualMachinesClient.Client)
	if err != nil {
		return err
	}
	return nil
}

type networkInterfaceClient interface {
	Get(ctx context.Context, resourceGroupName string, networkInterfaceName string, expand string) (network.Interface, error)
	ListAll(ctx context.Context) ([]network.Interface, error)
	CreateOrUpdate(ctx context.Context, resourceGroupName string, networkInterfaceName string, networkInterface network.Interface) (network.Interface, error)
	Delete(ctx context.Context, resourceGroupName string, networkInterfaceName string) error
}

type networkInterfaceClientWrapper struct {
	network.InterfacesClient
}

func (c *networkInterfaceClientWrapper) ListAll(ctx context.Context) ([]network.Interface, error) {
	listPage, err := c.InterfacesClient.ListAll(ctx)
	if err != nil {
		return nil, err
	}

	r := make([]network.Interface, 0)
	for listPage.NotDone() {
		networkInterfaces := listPage.Values()
		if 0 != len(networkInterfaces) {
			r = append(r, networkInterfaces...)
		}

		err = listPage.Next()
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (c *networkInterfaceClientWrapper) CreateOrUpdate(ctx context.Context, resourceGroupName string, networkInterfaceName string, networkInterface network.Interface) (network.Interface, error) {
	createFuture, err := c.InterfacesClient.CreateOrUpdate(ctx, resourceGroupName, networkInterfaceName, networkInterface)
	if err != nil {
		return network.Interface{}, err
	}

	err = createFuture.WaitForCompletion(ctx, c.InterfacesClient.Client)
	if err != nil {
		return network.Interface{}, err
	}

	networkInterface, err = createFuture.Result(c.InterfacesClient)
	return networkInterface, err
}

func (c *networkInterfaceClientWrapper) Delete(ctx context.Context, resourceGroupName string, networkInterfaceName string) error {
	deleteFuture, err := c.InterfacesClient.Delete(ctx, resourceGroupName, networkInterfaceName)
	if err != nil {
		if IsNotFound(err) {
			return nil
		}
		return err
	}

	err = deleteFuture.WaitForCompletion(ctx, c.InterfacesClient.Client)
	if err != nil {
		return err
	}
	return nil // TODO: 进一步检查 future 的返回值?
}
