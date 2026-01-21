package controller

import (
	"context"
	"fmt"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	kubevirtv1 "kubevirt.io/api/core/v1"

	"github.com/kubeovn/kube-ovn/pkg/informer"
	"github.com/kubeovn/kube-ovn/pkg/ovs"
	"github.com/kubeovn/kube-ovn/pkg/util"
)

func (c *Controller) enqueueAddVMIMigration(obj any) {
	key := cache.MetaObjectToName(obj.(*kubevirtv1.VirtualMachineInstanceMigration)).String()
	klog.Infof("enqueue add VMI migration %s", key)
	c.addOrUpdateVMIMigrationQueue.Add(key)
}

func (c *Controller) enqueueUpdateVMIMigration(oldObj, newObj any) {
	oldVmi := oldObj.(*kubevirtv1.VirtualMachineInstanceMigration)
	newVmi := newObj.(*kubevirtv1.VirtualMachineInstanceMigration)

	if !newVmi.DeletionTimestamp.IsZero() ||
		oldVmi.Status.Phase != newVmi.Status.Phase {
		key := cache.MetaObjectToName(newVmi).String()
		klog.Infof("enqueue update VMI migration %s", key)
		c.addOrUpdateVMIMigrationQueue.Add(key)
	}
}

func (c *Controller) enqueueDeleteVMIMigration(obj any) {
	klog.Infof("enqueueDeleteVMIMigration called, obj type: %T", obj)
	var vmiMigration *kubevirtv1.VirtualMachineInstanceMigration
	switch t := obj.(type) {
	case *kubevirtv1.VirtualMachineInstanceMigration:
		vmiMigration = t
	case cache.DeletedFinalStateUnknown:
		v, ok := t.Obj.(*kubevirtv1.VirtualMachineInstanceMigration)
		if !ok {
			klog.Warningf("unexpected object type: %T", t.Obj)
			return
		}
		vmiMigration = v
	default:
		klog.Warningf("unexpected object type: %T", obj)
		return
	}

	klog.Infof("enqueue delete VMI migration %s/%s for VMI %s", vmiMigration.Namespace, vmiMigration.Name, vmiMigration.Spec.VMIName)
	// Clean up LSP migrate options when VMIMigration is deleted
	c.handleDeleteVMIMigration(vmiMigration)
}

func (c *Controller) enqueueDeleteVM(obj any) {
	var vm *kubevirtv1.VirtualMachine
	switch t := obj.(type) {
	case *kubevirtv1.VirtualMachine:
		vm = t
	case cache.DeletedFinalStateUnknown:
		v, ok := t.Obj.(*kubevirtv1.VirtualMachine)
		if !ok {
			klog.Warningf("unexpected object type: %T", t.Obj)
			return
		}
		vm = v
	default:
		klog.Warningf("unexpected type: %T", obj)
		return
	}

	key := cache.MetaObjectToName(vm).String()
	klog.Infof("enqueue add VM %s", key)
	c.deleteVMQueue.Add(key)
}

func (c *Controller) handleDeleteVM(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid vm key: %s", key))
		return nil
	}
	vmKey := fmt.Sprintf("%s/%s", namespace, name)

	ports, err := c.OVNNbClient.ListNormalLogicalSwitchPorts(true, map[string]string{"pod": vmKey})
	if err != nil {
		klog.Errorf("failed to list lsps of vm %s: %v", vmKey, err)
		return err
	}

	for _, port := range ports {
		if err := c.config.KubeOvnClient.KubeovnV1().IPs().Delete(context.Background(), port.Name, metav1.DeleteOptions{}); err != nil {
			if !k8serrors.IsNotFound(err) {
				klog.Errorf("failed to delete ip %s, %v", port.Name, err)
				return err
			}
		}

		subnetName := port.ExternalIDs["ls"]
		if subnetName != "" {
			c.ipam.ReleaseAddressByNic(vmKey, port.Name, subnetName)
		}

		if err := c.OVNNbClient.DeleteLogicalSwitchPort(port.Name); err != nil {
			klog.Errorf("failed to delete lsp %s, %v", port.Name, err)
			return err
		}
	}

	return nil
}

func (c *Controller) handleAddOrUpdateVMIMigration(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	vmiMigration, err := c.config.KubevirtClient.VirtualMachineInstanceMigration(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			klog.V(3).Infof("VirtualMachineInstanceMigration %s not found, skipping", key)
			return nil
		}
		utilruntime.HandleError(fmt.Errorf("failed to get VMI migration by key %s: %w", key, err))
		return err
	}

	vmi, err := c.config.KubevirtClient.VirtualMachineInstance(namespace).Get(context.TODO(), vmiMigration.Spec.VMIName, metav1.GetOptions{})
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get VMI by name %s: %w", vmiMigration.Spec.VMIName, err))
		return err
	}

	// use VirtualMachineInstance's MigrationState because VirtualMachineInstanceMigration's MigrationState is not updated until migration finished
	var srcNodeName, targetNodeName string
	if vmi.Status.MigrationState != nil {
		klog.Infof("current vmiMigration %s status %s, target Node %s, source Node %s, target Pod %s, source Pod %s", key,
			vmiMigration.Status.Phase,
			vmi.Status.MigrationState.TargetNode,
			vmi.Status.MigrationState.SourceNode,
			vmi.Status.MigrationState.TargetPod,
			vmi.Status.MigrationState.SourcePod)
		srcNodeName = vmi.Status.MigrationState.SourceNode
		targetNodeName = vmi.Status.MigrationState.TargetNode
	} else {
		klog.Infof("current vmiMigration %s status %s, vmi MigrationState is nil", key, vmiMigration.Status.Phase)
	}

	portName := ovs.PodNameToPortName(vmiMigration.Spec.VMIName, vmiMigration.Namespace, util.OvnProvider)
	switch vmiMigration.Status.Phase {
	case kubevirtv1.MigrationScheduling:
		// Use kubevirt.io/created-by label to list all pods related to the VMI
		// because kubevirt.io/migrationJobUID label may not be set on target pod
		selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
			MatchLabels: map[string]string{
				"kubevirt.io/created-by": string(vmi.UID),
			},
		})
		if err != nil {
			err = fmt.Errorf("failed to create label selector for VMI %s: %w", vmi.UID, err)
			klog.Error(err)
			return err
		}

		pods, err := c.podsLister.Pods(vmiMigration.Namespace).List(selector)
		if err != nil {
			err = fmt.Errorf("failed to list pods for VMI %s: %w", vmi.UID, err)
			klog.Error(err)
			return err
		}

		if len(pods) > 0 {
			// During MigrationScheduling phase, always use vmi.Status.NodeName as source node
			// because vmi.Status.MigrationState may contain stale data from the previous migration
			sourceNode := vmi.Status.NodeName

			// Find target pod (running/pending pod scheduled on a different node than source)
			targetPod := pods[0]
			found := false
			for _, pod := range pods {
				// Skip completed/failed pods from previous migrations
				if pod.Status.Phase != "Running" && pod.Status.Phase != "Pending" {
					continue
				}
				if pod.Spec.NodeName != "" && pod.Spec.NodeName != sourceNode {
					targetPod = pod
					found = true
					break
				}
			}

			if !found {
				klog.Warningf("target pod not found for VMI %s/%s, source node: %s, pods count: %d",
					vmi.Namespace, vmi.Name, sourceNode, len(pods))
				return nil
			}

			if sourceNode == "" {
				klog.Warningf("VM pod %s/%s migration setup skipped, source node is empty (VMI: %s/%s)",
					targetPod.Namespace, targetPod.Name, vmi.Namespace, vmi.Name)
				return nil
			}

			klog.Infof("VM pod %s/%s is migrating from %s to %s (migration job UID: %s)",
				targetPod.Namespace, targetPod.Name, sourceNode, targetPod.Spec.NodeName, vmiMigration.UID)

			if err := c.OVNNbClient.SetLogicalSwitchPortMigrateOptions(portName, sourceNode, targetPod.Spec.NodeName); err != nil {
				err = fmt.Errorf("failed to set migrate options for VM pod lsp %s: %w", portName, err)
				klog.Error(err)
				return err
			}
			klog.Infof("successfully set migrate options for lsp %s from %s to %s", portName, sourceNode, targetPod.Spec.NodeName)
		} else {
			klog.Warningf("target pod not yet created for migration job UID %s in phase %s, waiting for pod creation",
				vmiMigration.UID, vmiMigration.Status.Phase)
			return nil
		}
	case kubevirtv1.MigrationSucceeded:
		// After migration succeeds, vmi.Status.MigrationState may be nil
		// Use vmi.Status.NodeName as the target node (VMI is now running on the new node)
		targetNode := targetNodeName
		if targetNode == "" {
			targetNode = vmi.Status.NodeName
		}
		klog.Infof("migrate end reset options for lsp %s from %s to %s, migrated succeed", portName, srcNodeName, targetNode)
		if err := c.OVNNbClient.ResetLogicalSwitchPortMigrateOptions(portName, srcNodeName, targetNode, false); err != nil {
			err = fmt.Errorf("failed to clean migrate options for lsp %s, %w", portName, err)
			klog.Error(err)
			return err
		}
	case kubevirtv1.MigrationFailed:
		klog.Infof("migrate end reset options for lsp %s from %s to %s, migrated fail", portName, srcNodeName, targetNodeName)
		if err := c.OVNNbClient.ResetLogicalSwitchPortMigrateOptions(portName, srcNodeName, targetNodeName, true); err != nil {
			err = fmt.Errorf("failed to clean migrate options for lsp %s, %w", portName, err)
			klog.Error(err)
			return err
		}
	}
	return nil
}

func (c *Controller) handleDeleteVMIMigration(vmiMigration *kubevirtv1.VirtualMachineInstanceMigration) {
	vmiName := vmiMigration.Spec.VMIName
	namespace := vmiMigration.Namespace
	portName := ovs.PodNameToPortName(vmiName, namespace, util.OvnProvider)

	// Get VMI to determine current node
	vmi, err := c.config.KubevirtClient.VirtualMachineInstance(namespace).Get(context.TODO(), vmiName, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			klog.Infof("VMI %s/%s not found, skip cleaning LSP migrate options", namespace, vmiName)
			return
		}
		klog.Errorf("failed to get VMI %s/%s: %v", namespace, vmiName, err)
		return
	}

	// Use VMI's current node as the target (where VM is running after migration)
	targetNode := vmi.Status.NodeName
	klog.Infof("VMI migration %s/%s deleted, resetting LSP %s options to node %s", namespace, vmiMigration.Name, portName, targetNode)

	if err := c.OVNNbClient.ResetLogicalSwitchPortMigrateOptions(portName, "", targetNode, false); err != nil {
		klog.Errorf("failed to reset migrate options for lsp %s on VMI migration delete: %v", portName, err)
	}
}

func (c *Controller) isKubevirtCRDInstalled() bool {
	for _, crd := range util.KubeVirtCRD {
		_, err := c.config.ExtClient.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), crd, metav1.GetOptions{})
		if err != nil {
			return false
		}
	}
	klog.Info("Found KubeVirt CRDs")
	return true
}

func (c *Controller) StartKubevirtInformerFactory(ctx context.Context, kubevirtInformerFactory informer.KubeVirtInformerFactory) {
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if c.isKubevirtCRDInstalled() {
					klog.Info("Start kubevirt informer")
					vmiMigrationInformer := kubevirtInformerFactory.VirtualMachineInstanceMigration()
					vmInformer := kubevirtInformerFactory.VirtualMachine()

					kubevirtInformerFactory.Start(ctx.Done())
					if !cache.WaitForCacheSync(ctx.Done(), vmiMigrationInformer.HasSynced, vmInformer.HasSynced) {
						util.LogFatalAndExit(nil, "failed to wait for kubevirt caches to sync")
					}

					if c.config.EnableLiveMigrationOptimize {
						if _, err := vmiMigrationInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
							AddFunc:    c.enqueueAddVMIMigration,
							UpdateFunc: c.enqueueUpdateVMIMigration,
							DeleteFunc: c.enqueueDeleteVMIMigration,
						}); err != nil {
							util.LogFatalAndExit(err, "failed to add VMI Migration event handler")
						}
						klog.Info("VMI Migration event handlers registered (Add, Update, Delete)")
					}

					if _, err := vmInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
						DeleteFunc: c.enqueueDeleteVM,
					}); err != nil {
						util.LogFatalAndExit(err, "failed to add vm event handler")
					}
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}
