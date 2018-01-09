package com.cisco.nm.vms.fw.policy.controller;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.cisco.nm.vms.api.xsd.FwOSMode;
import com.cisco.nm.vms.common.exception.VmsException;
import com.cisco.nm.vms.device.exception.VmsDeviceException;
import com.cisco.nm.vms.fw.common.fwpolicies.settings.VirtualContextPolicy;
import com.cisco.nm.vms.policy.BasePolicy;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * This is delegation class of ContextCacheController which keeps the cache 
 * and utilities for allocation among contexts and interfaces.
 * It should keep updating based on events of ContextCacheController.
 * 
 * Hence, it should be driven by ContextCacheController only.
 * 
 * @author rgauttam
 * 
 * @see     com.google.common.collect.Maps
 * @see     com.google.common.collect.Sets
 * @see     java.util.Iterator
 * @see     java.util.Map
 * @see     java.util.Set;
 *
 */
public class ContextCacheDelegate implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * This is set of interfaces[id] which have been allocated in a manner that
	 * they can't be used or shared by any other context for allocation.
	 * 
	 */
	private final Set<Long> notAvailableIntfSet = Sets.newConcurrentHashSet();

	/**
	 * It is many to many association map between contexts[id] and interfaces[id].
	 * There, it keeps an information about a context is associated with whose are the interfaces
	 * and an interface shared(allocated) among contexts.
	 */
	private final BidirectionalMap<Long, Long> ctxAndIntfAssociationMap = BidirectionalMap.newBidirectionalMap();

	private ContextCacheDelegate() {
	}

	/**
	 * @return a new instance of ContextCacheDelegate.
	 */
	public static ContextCacheDelegate createInstance() {
		return new ContextCacheDelegate();
	}

	/**
	 * It checks that given interface[id] is available or not for particular mode type of contexts. 
	 * 
	 * @param ctxId contextId
	 * @param mode contextMode
	 * @return
	 *  @throws IllegalArgumentException if any of parameter is null or not positive number.
	 */
	public Boolean isInterfaceAvailableForMode(Long intfId, FwOSMode mode) {
		if (intfId == null || mode == null || intfId <= 0)
			throw new IllegalArgumentException("Context mode and interface id are manadatory and greater than 0.");
		if (!notAvailableIntfSet.contains(intfId)) {
			if (FwOSMode.OS_MODE_TRANSPARENT.equals(mode)) {
				Set<Long> ctxSet = null;
				return (ctxSet = ctxAndIntfAssociationMap.getBySecond(intfId)) == null || ctxSet.isEmpty();
			} else if (FwOSMode.OS_MODE_ROUTED.equals(mode)) {
				return true;
			} else {
				throw new IllegalArgumentException("Only Routed or Transparent mode is applicaple.");
			}
		}
		return false;
	}

	/**
	 * It checks that the given interface[id] is available or not for given context[id] 
	 * which is a particular type of mode.
	 * 
	 * @param intfId interfaceId
	 * @param ctxId contextId
	 * @param mode contextMode
	 * @return
	 * @throws IllegalArgumentException if any of parameter is null or not positive number.
	 */
	public Boolean isInterfaceAvailableForContext(Long intfId, Long ctxId, FwOSMode mode) {
		if (ctxId == null || intfId == null || mode == null || intfId <= 0)
			throw new IllegalArgumentException("Context id, context mode and interface id are manadatory and greater than 0.");
		if (!notAvailableIntfSet.contains(intfId)) {
			if (FwOSMode.OS_MODE_TRANSPARENT.equals(mode)) {
				Set<Long> ctxSet = null;
				return (ctxSet = ctxAndIntfAssociationMap.getBySecond(intfId)) == null || ctxSet.isEmpty();
			} else if (FwOSMode.OS_MODE_ROUTED.equals(mode)) {
				return !ctxAndIntfAssociationMap.isEntryExist(ctxId, intfId);
			} else {
				throw new IllegalArgumentException("Only Routed or Transparent mode is applicaple.");
			}
		}
		return false;
	}

	/**
	 * It validates each base policy, in term of all interfaces in base policy are available or not 
	 * for this policy in current state of system interfaces. 
	 * If not, it throws an exception by having all interfaces names(as comma separated string) 
	 * which are not available for given type of policy.
	 * 
	 * Here, basically base policy is an instance of VirtualContextPolicy.
	 * 
	 * @param basePolicies
	 * 
	 * @throws IllegalArgumentException if any of the base policy is not a VirtualContextPolicy.
	 * @throws VmsDeviceException.INTERFACE_UNAVAILABLE_FOR_CONTEXT if any interface of any base policy 
	 *                                                              is not available for allocation.
	 */
	public void validateIntfertacesAvailability(BasePolicy[] basePolicies) throws VmsDeviceException {
		Set<String> alreadyAllocatedInterfaceNames = Sets.newConcurrentHashSet();
		for (int i = 0; i < basePolicies.length; i++) {
			if (basePolicies[i] instanceof VirtualContextPolicy) {
				VirtualContextPolicy vcPolicy = (VirtualContextPolicy) basePolicies[i];
				Map<Long, String> intfIdAndNameMap = getUnavailableInterfacesForContext(vcPolicy);
				if (!intfIdAndNameMap.isEmpty()) {
					alreadyAllocatedInterfaceNames.addAll(intfIdAndNameMap.values());
				}
			} else {
				throw new IllegalArgumentException("This operation is applicable for only VirtualContextPolicy type of base policies.");
			}
		}
		if (!alreadyAllocatedInterfaceNames.isEmpty()) {
			throw new VmsDeviceException.INTERFACE_UNAVAILABLE_FOR_CONTEXT(StringUtils.join(alreadyAllocatedInterfaceNames.toArray(), ", "));
		}
	}

	/**
	 * It validates virtual context policy, in term of all interfaces in this policy are available or not 
	 * in current state of system interfaces.
	 * If not, it throws an exception by having all interfaces names(as comma separated string) 
	 * which are not available for given type of virtual context policy.
	 * 
	 * @param vcPolicy
	 * @throws VmsException.INTERFACE_UNAVAILABLE_FOR_CONTEXT if any interface of given virtual context 
	 *                                                        is not available for allocation.
	 */
	public void validateIntfertacesAvailability(VirtualContextPolicy vcPolicy) throws VmsException {
		Map<Long, String> intfIdAndNameMap = getUnavailableInterfacesForContext(vcPolicy);
		if (!intfIdAndNameMap.isEmpty()) {
			throw new VmsDeviceException.INTERFACE_UNAVAILABLE_FOR_CONTEXT(StringUtils.join(intfIdAndNameMap.values().toArray(), ", "));
		}
	}

	/**
	 * @param vcPolicy
	 * @return Unavailable interfaces[id and name map] for particular VirtualContextPolicy.
	 */
	private Map<Long, String> getUnavailableInterfacesForContext(VirtualContextPolicy vcPolicy) {

		if (vcPolicy != null && vcPolicy.getInterfaces() != null) {
			if (vcPolicy.getInterfaces().isEmpty()) {
				return Maps.newConcurrentMap();
			}
			Set<Long> alreadyAllocatedIntfSet = Sets.newHashSet();
			Map<Long, String> intfIdAndNameMap = Maps.newConcurrentMap();
			vcPolicy.getInterfaces().forEach(ctxIntf -> intfIdAndNameMap.put(ctxIntf.getId(), ctxIntf.getName()));
			Set<Long> intfIdsSet = intfIdAndNameMap.keySet();
			if (FwOSMode.OS_MODE_TRANSPARENT.equals(getFwOSModeByValue(vcPolicy.getContextMode()))) {
				Set<Long> alreadyAllocatedIntfInCurrentCtx = ctxAndIntfAssociationMap.getByFirst(vcPolicy.getId());
				Set<Long> allAlreadyAllocatedIntfSet = ctxAndIntfAssociationMap.getSecondKeySet();
				if (alreadyAllocatedIntfInCurrentCtx == null) {
					alreadyAllocatedIntfSet.addAll(Sets.intersection(allAlreadyAllocatedIntfSet, intfIdsSet));
				} else {
					alreadyAllocatedIntfSet.addAll(Sets.intersection(
							Sets.difference(allAlreadyAllocatedIntfSet, alreadyAllocatedIntfInCurrentCtx), intfIdsSet));
				}
			} else {
				alreadyAllocatedIntfSet.addAll(Sets.intersection(notAvailableIntfSet, intfIdsSet));
			}
			if (alreadyAllocatedIntfSet.isEmpty()) {
				return Maps.newConcurrentMap();
			}
			Sets.difference(intfIdsSet, alreadyAllocatedIntfSet)
					.forEach(ctxIntfId -> intfIdAndNameMap.remove(ctxIntfId));
			return intfIdAndNameMap;
		} else
			throw new RuntimeException("Invalid parameters! All parameters are manadatory");

	}

	/**
	 * @param mode
	 * @return All already allocated interfaces[id] for given mode type of context.
	 */
	public Set<Long> getAllocatedInterfacesForMode(FwOSMode mode) {
		if (FwOSMode.OS_MODE_TRANSPARENT.equals(mode)) {
			return notAvailableIntfSet;
		} else {
			Set<Long> allUsedIntfSet = ctxAndIntfAssociationMap.getSecondKeySet();
			return allUsedIntfSet == null ? Sets.newHashSet() : Sets.difference(allUsedIntfSet, notAvailableIntfSet);
		}
	}

	/**
	 * @param mode
	 * @return All unavailable interfaces[id] for given mode type of context.
	 */
	public Set<Long> getAllUnavailableIntferfacesForMode(FwOSMode mode) {
		return getAllUnavailableIntferfacesForMode(null, mode);
	}

	/**
	 * @param contextId
	 * @param mode
	 * @return Unavailable interfaces[id] for a context id which is in given mode.
	 */
	public Set<Long> getAllUnavailableIntferfacesForMode(Long contextId, FwOSMode mode) {
		if (mode == null)
			return null;
		if (FwOSMode.OS_MODE_ROUTED.equals(mode)) {
			if (contextId == null)
				return notAvailableIntfSet;
			Set<Long> allUsedIntfSet = ctxAndIntfAssociationMap.getByFirst(contextId);
			return allUsedIntfSet == null ? notAvailableIntfSet : Sets.union(notAvailableIntfSet, allUsedIntfSet);
		} else if (FwOSMode.OS_MODE_TRANSPARENT.equals(mode)) {
			Set<Long> allUsedIntfSet = ctxAndIntfAssociationMap.getSecondKeySet();
			return allUsedIntfSet;
		}
		return null;
	}

	/**
	 * @param contextId
	 * @return All allocated interfaces[id] with given context[id].
	 */
	public Set<Long> getAllocatedInterfacesForContext(Long contextId) {
		return contextId != null ? ctxAndIntfAssociationMap.getByFirst(contextId) : null;
	}

	/**
	 * @param interfaceId
	 * @return All contexts[id] which are sharing given interface[id].
	 */
	public Set<Long> getAssociatedContextsForInterface(Long interfaceId) {
		return interfaceId != null ? ctxAndIntfAssociationMap.getBySecond(interfaceId) : null;
	}

	/**
	 * It loads this ContextCacheDelegate freshly.
	 * 
	 * @param notAvailableIntfSet The interface[id] set which are not available for any new allocation at all.
	 * @param ctxAssignedIntfSetMap The map of contexts[id] and their already allocated interfaces[id] set.
	 */
	void onLoad(Set<Long> notAvailableIntfSet, Map<Long, Set<Long>> ctxAssignedIntfSetMap) {
		clear();
		this.notAvailableIntfSet.addAll(notAvailableIntfSet);
		ctxAssignedIntfSetMap.entrySet().forEach(entry -> {
			if (entry.getValue() != null && !entry.getValue().isEmpty())
				this.ctxAndIntfAssociationMap.putAll(entry.getKey(), entry.getValue());
		});
	}

	/**
	 * It clears every existing entries and resets ContextCacheDelegate.
	 */
	public void clear() {
		this.notAvailableIntfSet.clear();
		this.ctxAndIntfAssociationMap.clear();
	}

	/**
	 * It updates ContextCacheDelegate on delete of given context[id]
	 * 
	 * @param contextId
	 */
	void onDeleteContext(Long contextId) {
		if (contextId != null) {
			Set<Long> intfs = ctxAndIntfAssociationMap.getByFirst(contextId);
			if (intfs != null) {
				notAvailableIntfSet.removeAll(intfs);
			}
			ctxAndIntfAssociationMap.removeByFirst(contextId);
		}
	}

	/**
	 * It updates ContextCacheDelegate on update of any interface allocation or deallocation
	 *  of given existing VirtualContextPolicy.
	 * 
	 * @param vcPolicy
	 */
	void onUpdateContext(VirtualContextPolicy vcPolicy) {
			onCreateOrUpdateCtx(vcPolicy, true);
	}

	/**
	 * It updates ContextCacheDelegate on creation of VirtualContextPolicies.
	 * 
	 * Here, basePolices should be instance of VirtualContextPolicies.
	 *  
	 * @param basePolicies array of VirtualContextPolicy
	 */
	void onCreateContext(BasePolicy[] basePolicies) {
		for (BasePolicy basePolicy : basePolicies) {
			onCreateOrUpdateCtx((VirtualContextPolicy) basePolicy, false);
		}
	}

	private void onCreateOrUpdateCtx(VirtualContextPolicy vcPolicy, boolean isUpdateHandler) {
		if (isUpdateHandler)
			onDeleteContext(vcPolicy.getId());
		if (vcPolicy.getInterfaces() != null && !vcPolicy.getInterfaces().isEmpty()) {
			Set<Long> intfIdsSet = Sets.newConcurrentHashSet();
			vcPolicy.getInterfaces().forEach(ctxIntf -> intfIdsSet.add(ctxIntf.getId()));

			if (FwOSMode.OS_MODE_TRANSPARENT.equals(getFwOSModeByValue(vcPolicy.getContextMode()))) {
				notAvailableIntfSet.addAll(intfIdsSet);
			}
			intfIdsSet.forEach(intfId -> ctxAndIntfAssociationMap.put(vcPolicy.getId(), intfId));
		}
	}

	/**
	 * value could be either Routed or Transparent.
	 * 
	 * @param value
	 * @return FwOSMode
	 */
	public static FwOSMode getFwOSModeByValue(String value) {
		if (value.equalsIgnoreCase("Routed"))
			return FwOSMode.OS_MODE_ROUTED;
		if (value.equalsIgnoreCase("Transparent"))
			return FwOSMode.OS_MODE_TRANSPARENT;
		throw new IllegalArgumentException("Only Routed or Transparent values are applicaple.");
	}
}