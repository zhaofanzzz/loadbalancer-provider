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
	"html/template"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTemplate(t *testing.T) {
	vs1 := &virtualServer{
		AcceptMark: 1,
		VIP:        "1.1.1.100",
		Scheduler:  "rr",
		RealServer: []string{"1.1.1.1", "1.1.1.2", "1.1.1.3"},
	}
	vi1 := &vrrpInstance{
		State:     "BACKUP",
		Vrid:      1,
		Priority:  100,
		Interface: "eth1",
		MyIP:      "1.1.1.2",
		AllIPs:    []string{"1.1.1.1", "1.1.1.2", "1.1.1.3"},
		VIP:       "1.1.1.100",
	}

	vi2 := &vrrpInstance{
		State:     "BACKUP",
		Vrid:      2,
		Priority:  200,
		Interface: "eth2",
		MyIP:      "2.1.1.1",
		AllIPs:    []string{"2.1.1.1", "2.1.1.5", "2.1.1.7"},
		VIP:       "2.1.1.100",
	}

	vss := []*virtualServer{vs1}
	vis := []*vrrpInstance{vi1, vi2}

	tmpl, _ := template.ParseFiles("../../build/ipvsdr/keepalived.tmpl")
	conf := make(map[string]interface{})

	conf["iptablesChain"] = iptablesChain
	conf["instances"] = vis
	conf["vss"] = vss

	assert.Nil(t, tmpl.Execute(ioutil.Discard, conf))
}
