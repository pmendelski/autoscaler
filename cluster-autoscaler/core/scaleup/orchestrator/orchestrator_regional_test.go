/*
Copyright 2016 The Kubernetes Authors.
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

package orchestrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	testcloudprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	. "k8s.io/autoscaler/cluster-autoscaler/core/test"
	"k8s.io/autoscaler/cluster-autoscaler/core/utils"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodeinfosprovider"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/kubernetes/fake"
	kube_record "k8s.io/client-go/tools/record"
)

// NodeGroupConfig is a node group config used in tests
type NodeGroupConfig struct {
	Name    string
	MinSize int
	MaxSize int
}

// GroupsScaleUpTestConfig represents a config of a scale test
type GroupsScaleUpTestConfig struct {
	Groups    []NodeGroupConfig
	Nodes     []NodeConfig
	Pods      []PodConfig
	ExtraPods []PodConfig
	OnScaleUp testcloudprovider.OnScaleUpFunc
	Options   *config.AutoscalingOptions
}

// GroupsScaleUpTestResult represents a node groups scale up result
type GroupsScaleUpTestResult struct {
	Error            errors.AutoscalerError
	ScaleUpStatus    ScaleUpStatusInfo
	GroupSizeChanges []GroupSizeChange
	Events           []string
	TargetSizes      map[string]int
}

func TestScaleRegionalToOtherNodeGroupWhenFirstIsFilled(t *testing.T) {
	options := defaultOptions
	options.MaxCoresTotal = 100 * 1000
	options.MaxMemoryTotal = 100 * 1000 * utils.MiB
	// Test passes with:
	// options.BalanceSimilarNodeGroups = true
	config := &GroupsScaleUpTestConfig{
		Groups: []NodeGroupConfig{
			{Name: "ng1", MaxSize: 2},
			{Name: "ng2", MaxSize: 2},
		},
		Nodes: []NodeConfig{
			{Name: "ng1-n1", Cpu: 1500, Memory: 1000 * utils.MiB, Ready: true, Group: "ng1"},
			{Name: "ng2-n1", Cpu: 1500, Memory: 1000 * utils.MiB, Ready: true, Group: "ng2"},
		},
		Pods: []PodConfig{
			{Name: "p1", Cpu: 1400, Node: "ng1-n1"},
			{Name: "p2", Cpu: 1400, Node: "ng2-n1"},
		},
		ExtraPods: []PodConfig{
			{Name: "p3", Cpu: 1400},
			{Name: "p4", Cpu: 1400},
		},
		Options: &options,
	}
	expectedTargetSizes := map[string]int{
		"ng1": 2,
		"ng2": 2,
	}
	result := scaleUpGroups(t, config)
	assert.True(t, result.ScaleUpStatus.WasSuccessful())
	assert.Nil(t, result.Error)
	assert.Equal(t, expectedTargetSizes, result.TargetSizes)
	// Actual:
	// result.TargetSized = map[string]int{
	//   "ng1": 2,
	//   "ng2": 1,
	// }
}

func scaleUpGroups(t *testing.T, config *GroupsScaleUpTestConfig) *GroupsScaleUpTestResult {
	now := time.Now()
	groupSizeChangesChannel := make(chan GroupSizeChange, 20)
	groupNodes := make(map[string][]*apiv1.Node)

	// build nodes
	nodes := make([]*apiv1.Node, 0, len(config.Nodes))
	for _, n := range config.Nodes {
		node := BuildTestNode(n.Name, n.Cpu, n.Memory)
		if n.Gpu > 0 {
			AddGpusToNode(node, n.Gpu)
		}
		SetNodeReadyState(node, n.Ready, now.Add(-2*time.Minute))
		nodes = append(nodes, node)
		if n.Group != "" {
			groupNodes[n.Group] = append(groupNodes[n.Group], node)
		}
	}

	// build and setup pods
	pods := make([]*apiv1.Pod, len(config.Pods))
	for i, p := range config.Pods {
		pods[i] = buildTestPod(p)
	}
	extraPods := make([]*apiv1.Pod, len(config.ExtraPods))
	for i, p := range config.ExtraPods {
		extraPods[i] = buildTestPod(p)
	}
	podLister := kube_util.NewTestPodLister(pods)
	listers := kube_util.NewListerRegistry(nil, nil, podLister, nil, nil, nil, nil, nil, nil, nil)

	// setup node groups
	provider := testcloudprovider.NewTestCloudProvider(func(nodeGroup string, increase int) error {
		groupSizeChangesChannel <- GroupSizeChange{GroupName: nodeGroup, SizeChange: increase}
		if config.OnScaleUp != nil {
			return config.OnScaleUp(nodeGroup, increase)
		}
		return nil
	}, nil)
	options := defaultOptions
	if config.Options != nil {
		options = *config.Options
	}
	resourceLimiter := cloudprovider.NewResourceLimiter(
		map[string]int64{cloudprovider.ResourceNameCores: options.MinCoresTotal, cloudprovider.ResourceNameMemory: options.MinMemoryTotal},
		map[string]int64{cloudprovider.ResourceNameCores: options.MaxCoresTotal, cloudprovider.ResourceNameMemory: options.MaxMemoryTotal})
	provider.SetResourceLimiter(resourceLimiter)
	groupConfigs := make(map[string]*NodeGroupConfig)
	for _, group := range config.Groups {
		groupConfigs[group.Name] = &group
	}
	for name, nodesInGroup := range groupNodes {
		groupConfig := groupConfigs[name]
		if groupConfig == nil {
			groupConfig = &NodeGroupConfig{
				Name:    name,
				MinSize: 1,
				MaxSize: 10,
			}
		}
		provider.AddNodeGroup(name, groupConfig.MinSize, groupConfig.MaxSize, len(nodesInGroup))
		for _, n := range nodesInGroup {
			provider.AddNode(name, n)
		}
	}

	// build orchestrator
	context, _ := NewScaleTestAutoscalingContext(options, &fake.Clientset{}, listers, provider, nil, nil)
	nodeInfos, _ := nodeinfosprovider.NewDefaultTemplateNodeInfoProvider(nil, false).
		Process(&context, nodes, []*appsv1.DaemonSet{}, taints.TaintConfig{}, now)
	clusterState := clusterstate.
		NewClusterStateRegistry(provider, clusterstate.ClusterStateRegistryConfig{}, context.LogRecorder, NewBackoff(), clusterstate.NewStaticMaxNodeProvisionTimeProvider(15*time.Minute))
	clusterState.UpdateNodes(nodes, nodeInfos, time.Now())
	processors := NewTestProcessors(&context)
	orchestrator := New()
	orchestrator.Initialize(&context, processors, clusterState, taints.TaintConfig{})
	expander := reportingStrategy{
		initialNodeConfigs: config.Nodes,
		results:            &expanderResults{},
		t:                  t,
	}
	context.ExpanderStrategy = expander

	// scale up
	scaleUpStatus, err := orchestrator.ScaleUp(extraPods, nodes, []*appsv1.DaemonSet{}, nodeInfos)
	processors.ScaleUpStatusProcessor.Process(&context, scaleUpStatus)

	// aggregate group size changes
	close(groupSizeChangesChannel)
	var groupSizeChanges []GroupSizeChange
	for change := range groupSizeChangesChannel {
		groupSizeChanges = append(groupSizeChanges, change)
	}

	// aggregate events
	eventsChannel := context.Recorder.(*kube_record.FakeRecorder).Events
	close(eventsChannel)
	events := []string{}
	for event := range eventsChannel {
		events = append(events, event)
	}

	// build target sizes
	targetSizes := make(map[string]int)
	for _, group := range provider.NodeGroups() {
		targetSizes[group.Id()], _ = group.TargetSize()
	}

	return &GroupsScaleUpTestResult{
		Error:            err,
		ScaleUpStatus:    simplifyScaleUpStatus(scaleUpStatus),
		GroupSizeChanges: groupSizeChanges,
		Events:           events,
		TargetSizes:      targetSizes,
	}
}
