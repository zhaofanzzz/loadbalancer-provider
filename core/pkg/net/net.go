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

package net

import (
	"fmt"
	"regexp"

	"github.com/zoumo/golib/netutil"
)

// Interface represents the local network interface
type Interface = netutil.Interface

var (
	invalidIfaces = []string{"docker0", "flannel.1", "cbr0"}
	vethRegex     = regexp.MustCompile(`^veth.*`)
)

// filter lo, docker0, flannel.1 and veth interfaces.
func filter(iface Interface) bool {
	if iface.IsLoopback() ||
		vethRegex.MatchString(iface.Name) ||
		stringInSlice(iface.Name, invalidIfaces) {
		return true
	}
	return false
}

// InterfaceByLoopback returns the loopback interface
func InterfaceByLoopback() (*Interface, error) {
	slice, err := netutil.InterfacesByLoopback()
	if err != nil {
		return nil, err
	}

	one := slice.One()
	if one == nil {
		return nil, fmt.Errorf("Can not find loopback interface")
	}

	return one, nil
}

// InterfaceByIP returns the local network interface that is using the
// specified IP address.
func InterfaceByIP(ip string) (*Interface, error) {
	slice, err := netutil.InterfacesByIP(ip)
	if err != nil {
		return nil, err
	}

	slice = slice.Filter(filter)

	one := slice.One()
	if one == nil {
		return nil, fmt.Errorf("Can not find net interface for ip:%v", ip)
	}
	return one, err
}

// StringInSlice check whether string a is a member of slice.
func stringInSlice(a string, slice []string) bool {
	for _, b := range slice {
		if b == a {
			return true
		}
	}
	return false
}
