package com.cisco.nm.vms.fw.policy.controller;

import static com.cisco.nm.vms.fw.policy.controller.ContextCacheDelegate.getFwOSModeByValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import com.cisco.nm.vms.api.xsd.FwOSMode;
import com.cisco.nm.vms.common.exception.VmsException;
import com.cisco.nm.vms.device.exception.VmsDeviceException;
import com.cisco.nm.vms.fw.common.fwpolicies.settings.ContextInterface;
import com.cisco.nm.vms.fw.common.fwpolicies.settings.VirtualContextPolicy;
import com.cisco.nm.vms.policy.BasePolicy;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@RunWith(PowerMockRunner.class)
public class ContextCacheDelegateTest {

	private static final long ROUTED_CTX_ID_NEW = 1l;
	private static final long TRANSPARENT_ID_NEW = 2l;
	private static final long ROUTED_CTX_ID_1 = 81l;
	private static final long ROUTED_CTX_ID_2 = 82l;
	private static final long TRANSPARENT_ID_1 = 9l;
	private static final long TRANSPARENT_ID_2 = 10l;
	private static final FwOSMode ROUTED_CTX_MODE = getFwOSModeByValue("Routed");
	private static final FwOSMode TRANSPARENT_CTX_MODE = getFwOSModeByValue("Transparent");
	private ContextCacheDelegate delegate = null;
	private Set<Long> allIntfSet;
	private Set<Long> notAvailableIntfSet;

	@Before
	public void setUp() {
		delegate = ContextCacheDelegate.createInstance();
	}

	private void createSeedData() {
		allIntfSet = Sets.newConcurrentHashSet();
		allIntfSet.add(101l);
		allIntfSet.add(102l);
		allIntfSet.add(103l);
		allIntfSet.add(201l);
		allIntfSet.add(202l);
		allIntfSet.add(301l);
		allIntfSet.add(601l);
		allIntfSet.add(801l);
		allIntfSet.add(802l);
		allIntfSet.add(803l);
		allIntfSet.add(901l);
		allIntfSet.add(902l);
		allIntfSet.add(903l);

		// Interfaces Set For Transparent Ctx
		notAvailableIntfSet = Sets.newConcurrentHashSet();
		notAvailableIntfSet.add(901l);
		notAvailableIntfSet.add(902l);
		notAvailableIntfSet.add(903l);

		// Interfaces Set For Routed Ctx
		Set<Long> intfSetForRoutedCtx1 = Sets.newConcurrentHashSet();
		intfSetForRoutedCtx1.add(801l);
		intfSetForRoutedCtx1.add(802l);
		Set<Long> intfSetForRoutedCtx2 = Sets.newConcurrentHashSet();
		intfSetForRoutedCtx2.add(803l);

		Map<Long, Set<Long>> ctxAssignedIntfSetMap = Maps.newConcurrentMap();
		ctxAssignedIntfSetMap.put(TRANSPARENT_ID_1, notAvailableIntfSet);
		ctxAssignedIntfSetMap.put(ROUTED_CTX_ID_1, intfSetForRoutedCtx1);
		ctxAssignedIntfSetMap.put(ROUTED_CTX_ID_2, intfSetForRoutedCtx2);

		delegate.onLoad(notAvailableIntfSet, ctxAssignedIntfSetMap);
	}

	@Test
	public void getAllUnavailableIntferfacesForModeTest() {
		createSeedData();
		assertEquals(delegate.getAllUnavailableIntferfacesForMode(TRANSPARENT_CTX_MODE).size(), 6);
		assertEquals(delegate.getAllUnavailableIntferfacesForMode(ROUTED_CTX_MODE).size(), 3);

		assertEquals(delegate.getAllUnavailableIntferfacesForMode(ROUTED_CTX_ID_1, ROUTED_CTX_MODE).size(), 5);
	}

	@Test
	public void getAllocatedInterfacesForModeTest() {
		createSeedData();
		assertEquals(delegate.getAllocatedInterfacesForMode(TRANSPARENT_CTX_MODE).size(), 3);
		assertEquals(delegate.getAllocatedInterfacesForMode(ROUTED_CTX_MODE).size(), 3);
	}

	@Test
	public void isInterfaceAvailableForModeTest() {
		createSeedData();
		assertTrue(delegate.isInterfaceAvailableForMode(101l, ROUTED_CTX_MODE));
		assertTrue(delegate.isInterfaceAvailableForMode(101l, TRANSPARENT_CTX_MODE));
		assertTrue(delegate.isInterfaceAvailableForMode(801l, ROUTED_CTX_MODE));
		assertFalse(delegate.isInterfaceAvailableForMode(901l, ROUTED_CTX_MODE));
		assertFalse(delegate.isInterfaceAvailableForMode(801l, TRANSPARENT_CTX_MODE));
		assertFalse(delegate.isInterfaceAvailableForMode(901l, TRANSPARENT_CTX_MODE));
	}

	@Test
	public void isInterfaceAvailableForContextTest() {
		createSeedData();

		assertTrue(delegate.isInterfaceAvailableForContext(801l, ROUTED_CTX_ID_2, ROUTED_CTX_MODE));
		assertTrue(delegate.isInterfaceAvailableForContext(101l, ROUTED_CTX_ID_1, ROUTED_CTX_MODE));
		assertTrue(delegate.isInterfaceAvailableForContext(201l, TRANSPARENT_ID_1, TRANSPARENT_CTX_MODE));
		assertFalse(delegate.isInterfaceAvailableForContext(901l, TRANSPARENT_ID_1, TRANSPARENT_CTX_MODE));
		assertFalse(delegate.isInterfaceAvailableForContext(901l, TRANSPARENT_ID_2, TRANSPARENT_CTX_MODE));
		assertFalse(delegate.isInterfaceAvailableForContext(801l, TRANSPARENT_ID_1, TRANSPARENT_CTX_MODE));
	}

	@Test
	public void createRoutedContextTest() throws VmsException {
		int allAvailableIntfForRoutedCtxPreCreateCtx = createContext(ROUTED_CTX_ID_NEW, ROUTED_CTX_MODE);
		int allAvailableIntfForRoutedCtx = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(ROUTED_CTX_MODE).size();
		int allIntf = allIntfSet.size();

		assertEquals(allAvailableIntfForRoutedCtxPreCreateCtx, allAvailableIntfForRoutedCtx);

		allAvailableIntfForRoutedCtx = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(ROUTED_CTX_ID_NEW, ROUTED_CTX_MODE).size();
		assertEquals(allAvailableIntfForRoutedCtxPreCreateCtx - 3, allAvailableIntfForRoutedCtx);

		allAvailableIntfForRoutedCtx = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(ROUTED_CTX_ID_1, ROUTED_CTX_MODE).size();
		assertEquals(allAvailableIntfForRoutedCtxPreCreateCtx - 2, allAvailableIntfForRoutedCtx);

		allAvailableIntfForRoutedCtx = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(TRANSPARENT_ID_1, TRANSPARENT_CTX_MODE).size();
		assertEquals(allIntf - 9, allAvailableIntfForRoutedCtx);

		allAvailableIntfForRoutedCtx = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(TRANSPARENT_CTX_MODE).size();
		assertEquals(allIntf - 9, allAvailableIntfForRoutedCtx);

	}

	@Test
	public void createTransparentContextTest() throws VmsException {
		int allAvailableIntfPreCreateCtx = createContext(TRANSPARENT_ID_NEW, TRANSPARENT_CTX_MODE);
		// Total 13 intf here from seed data. in which 3 already associated to a
		// transparent
		// ctx and other 3 intfs are associated to 2 different routed contexts.
		// So only 7 intfs are available for transparent ctx
		// As a part of this test case 3 more intf are being assigned to new
		// transparent ctx
		// So only 4 intfs are available for transparent ctx
		int allAvailableIntf = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(TRANSPARENT_CTX_MODE).size();
		assertEquals(allAvailableIntfPreCreateCtx - 3, allAvailableIntf);

		// So only 4 intfs are available for transparent ctx. As Transparent ctx
		// 2l doesn't have any intf.
		allAvailableIntf = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(2l, TRANSPARENT_CTX_MODE).size();
		assertEquals(allAvailableIntfPreCreateCtx - 3, allAvailableIntf);

		// As Routed ctx can share intfs. So now 7 intfs are available for
		// routed ctx
		allAvailableIntf = allIntfSet.size() - delegate.getAllUnavailableIntferfacesForMode(ROUTED_CTX_MODE).size();
		assertEquals(allAvailableIntfPreCreateCtx, allAvailableIntf);

		// As Routed ctx can share intfs. So now 7 intfs are available for
		// routed ctx. As Routed ctx 3l doesn't have any intf.
		allAvailableIntf = allIntfSet.size() - delegate.getAllUnavailableIntferfacesForMode(3l, ROUTED_CTX_MODE).size();
		assertEquals(allAvailableIntfPreCreateCtx, allAvailableIntf);

		// As Routed ctx ROUTED_CTX_ID_1 has 2 intfs. So 5 intfs available for
		// Ctx ROUTED_CTX_ID_1
		allAvailableIntf = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(ROUTED_CTX_ID_1, ROUTED_CTX_MODE).size();
		assertEquals(allAvailableIntfPreCreateCtx - 2, allAvailableIntf);

		// As Routed ctx ROUTED_CTX_ID_2 has 1 intfs. So 6 intfs available for
		// Ctx ROUTED_CTX_ID_2
		allAvailableIntf = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(ROUTED_CTX_ID_2, ROUTED_CTX_MODE).size();
		assertEquals(allAvailableIntfPreCreateCtx - 1, allAvailableIntf);

		// As Transparent ctx TRANSPARENT_ID_NEW has 3 intfs. So 4 intfs
		// available for Ctx TRANSPARENT_ID_NEW
		allAvailableIntf = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(TRANSPARENT_ID_NEW, TRANSPARENT_CTX_MODE).size();
		assertEquals(allAvailableIntfPreCreateCtx - 3, allAvailableIntf);
	}

	@Test
	public void createRoutedContextWithInterfacesSharingTest() throws VmsException {
		createSeedData();
		int allAvailableIntfPreCreateCtx = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(TRANSPARENT_CTX_MODE).size();
		createContextWithoutSeedData(100l, ROUTED_CTX_MODE);
		createContextWithoutSeedData(101l, ROUTED_CTX_MODE);
		int allAvailableIntf = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(TRANSPARENT_CTX_MODE).size();
		assertEquals(allAvailableIntfPreCreateCtx - 3, allAvailableIntf);
		assertNotNull(delegate.getAllocatedInterfacesForContext(100l));
		assertNotNull(delegate.getAllocatedInterfacesForContext(101l));
		assertEquals(3, delegate.getAllocatedInterfacesForContext(100l).size());
		assertEquals(3, delegate.getAllocatedInterfacesForContext(101l).size());
	}

	@Test
	public void createTransparentContextWithInterfacesSharingTest() throws VmsException {
		createSeedData();
		int allAvailableIntfPreCreateCtx = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(TRANSPARENT_CTX_MODE).size();
		createContextWithoutSeedData(100l, TRANSPARENT_CTX_MODE);
		int allAvailableIntf = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(TRANSPARENT_CTX_MODE).size();
		assertEquals(allAvailableIntfPreCreateCtx - 3, allAvailableIntf);
		assertNotNull(delegate.getAllocatedInterfacesForContext(100l));
		assertEquals(3, delegate.getAllocatedInterfacesForContext(100l).size());
		try {

			createContextWithoutSeedData(101l, TRANSPARENT_CTX_MODE);
			fail();
		} catch (VmsDeviceException.INTERFACE_UNAVAILABLE_FOR_CONTEXT e) {
			assertTrue(!e.getUnavailableInterfaces().isEmpty());
		}
	}

	private int createContext(Long ctxId, FwOSMode contextMode) throws VmsException {
		createSeedData();
		int allAvailableIntfPreCreateCtx = createContextWithoutSeedData(ctxId, contextMode);
		return allAvailableIntfPreCreateCtx;
	}

	private int createContextWithoutSeedData(Long ctxId, FwOSMode contextMode) throws VmsException {
		int allAvailableIntfPreCreateCtx = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(contextMode).size();
		Set<Long> intfSet = Sets.newConcurrentHashSet();
		intfSet.add(101l);
		intfSet.add(102l);
		intfSet.add(103l);

		delegate.validateIntfertacesAvailability(
				getVirtualContextPolicy(ctxId, getContextModeValue(contextMode), intfSet));
		delegate.onCreateContext(
				new BasePolicy[] { getVirtualContextPolicy(ctxId, getContextModeValue(contextMode), intfSet) });
		return allAvailableIntfPreCreateCtx;
	}

	@Test
	public void createContextNegativeTest() throws VmsException {
		createSeedData();
		Set<Long> intfSet = Sets.newHashSet();
		intfSet.add(101l);
		intfSet.add(102l);
		intfSet.add(801l);
		intfSet.add(901l);
		try {

			delegate.validateIntfertacesAvailability(
					getVirtualContextPolicy(ROUTED_CTX_ID_NEW, getContextModeValue(ROUTED_CTX_MODE), intfSet));
			delegate.onCreateContext(new BasePolicy[] {
					getVirtualContextPolicy(ROUTED_CTX_ID_NEW, getContextModeValue(ROUTED_CTX_MODE), intfSet) });
			fail();
		} catch (VmsDeviceException.INTERFACE_UNAVAILABLE_FOR_CONTEXT e) {
			assertTrue(!e.getUnavailableInterfaces().isEmpty());
		}
		try {
			delegate.validateIntfertacesAvailability(
					getVirtualContextPolicy(TRANSPARENT_ID_NEW, getContextModeValue(TRANSPARENT_CTX_MODE), intfSet));
			delegate.onCreateContext(new BasePolicy[] {
					getVirtualContextPolicy(TRANSPARENT_ID_NEW, getContextModeValue(TRANSPARENT_CTX_MODE), intfSet) });
			fail();
		} catch (VmsDeviceException.INTERFACE_UNAVAILABLE_FOR_CONTEXT e) {
			assertTrue(!e.getUnavailableInterfaces().isEmpty());
		}

	}

	private VirtualContextPolicy getVirtualContextPolicy(long vcPolicyId, String contextMode, Set<Long> intfSet) {
		VirtualContextPolicy vcPolicy = mock(VirtualContextPolicy.class);
		when(vcPolicy.getId()).thenReturn(vcPolicyId);
		when(vcPolicy.getContextMode()).thenReturn(contextMode);
		List<ContextInterface> interfaces = Lists.newArrayList();
		intfSet.forEach(intfId -> interfaces.add(getContextInterface(intfId)));
		when(vcPolicy.getInterfaces()).thenReturn(interfaces);
		return vcPolicy;
	}

	private ContextInterface getContextInterface(long interfaceId) {
		ContextInterface contextInterface = mock(ContextInterface.class);
		when(contextInterface.getId()).thenReturn(interfaceId);
		when(contextInterface.getName()).thenReturn("interfaceName-" + interfaceId);
		return contextInterface;
	}

	@Test
	public void updateRoutedContextText() {
		int allAvailableIntfForRoutedCtxPreUpdateCtx = updateContext(ROUTED_CTX_ID_1, ROUTED_CTX_MODE);
		int allAvailableIntf = allIntfSet.size() - delegate.getAllUnavailableIntferfacesForMode(ROUTED_CTX_MODE).size();
		int allIntf = allIntfSet.size();

		assertEquals(allAvailableIntfForRoutedCtxPreUpdateCtx, allAvailableIntf);

		allAvailableIntf = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(ROUTED_CTX_ID_1, ROUTED_CTX_MODE).size();
		assertEquals(allAvailableIntfForRoutedCtxPreUpdateCtx - 3, allAvailableIntf);

		allAvailableIntf = allIntf
				- delegate.getAllUnavailableIntferfacesForMode(ROUTED_CTX_ID_2, ROUTED_CTX_MODE).size();
		assertEquals(allAvailableIntfForRoutedCtxPreUpdateCtx - 1, allAvailableIntf);

		allAvailableIntf = allIntf
				- delegate.getAllUnavailableIntferfacesForMode(TRANSPARENT_ID_1, TRANSPARENT_CTX_MODE).size();
		assertEquals(allIntf - 7, allAvailableIntf);

		allAvailableIntf = allIntf - delegate.getAllUnavailableIntferfacesForMode(TRANSPARENT_CTX_MODE).size();
		assertEquals(allIntf - 7, allAvailableIntf);
	}

	@Test
	public void updateTransparentContextTest() {
		int allAvailableIntfForTransparentCtxPreUpdateCtx = updateContext(TRANSPARENT_ID_2, TRANSPARENT_CTX_MODE);
		int allAvailableIntf = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(TRANSPARENT_CTX_MODE).size();
		assertEquals(allAvailableIntfForTransparentCtxPreUpdateCtx - 3, allAvailableIntf);
	}

	private int updateContext(Long ctxId, FwOSMode contextMode) {
		createSeedData();
		int allAvailableIntfPreCreateCtx = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(contextMode).size();
		Set<Long> intfSet = Sets.newConcurrentHashSet();
		intfSet.add(101l);
		intfSet.add(102l);
		intfSet.add(103l);
		delegate.onUpdateContext(getVirtualContextPolicy(ctxId, getContextModeValue(contextMode), intfSet));
		return allAvailableIntfPreCreateCtx;
	}

	@Test
	public void updateContextWithNegativeTest() throws VmsException {
		createSeedData();
		Set<Long> intfSet = Sets.newConcurrentHashSet();
		intfSet.add(101l);
		intfSet.add(102l);
		intfSet.add(801l);
		intfSet.add(901l);

		try {
			delegate.validateIntfertacesAvailability(
					getVirtualContextPolicy(ROUTED_CTX_ID_1, getContextModeValue(ROUTED_CTX_MODE), intfSet));
			delegate.onUpdateContext(
					getVirtualContextPolicy(ROUTED_CTX_ID_1, getContextModeValue(ROUTED_CTX_MODE), intfSet));
			fail();
		} catch (VmsDeviceException.INTERFACE_UNAVAILABLE_FOR_CONTEXT e) {
			assertTrue(!e.getUnavailableInterfaces().isEmpty());
		}

		try {
			delegate.validateIntfertacesAvailability(
					getVirtualContextPolicy(ROUTED_CTX_ID_2, getContextModeValue(ROUTED_CTX_MODE), intfSet));
			delegate.onUpdateContext(
					getVirtualContextPolicy(ROUTED_CTX_ID_2, getContextModeValue(ROUTED_CTX_MODE), intfSet));
			fail();
		} catch (VmsDeviceException.INTERFACE_UNAVAILABLE_FOR_CONTEXT e) {
			assertTrue(!e.getUnavailableInterfaces().isEmpty());
		}

		try {
			delegate.validateIntfertacesAvailability(
					getVirtualContextPolicy(TRANSPARENT_ID_1, getContextModeValue(TRANSPARENT_CTX_MODE), intfSet));
			delegate.onUpdateContext(
					getVirtualContextPolicy(TRANSPARENT_ID_1, getContextModeValue(TRANSPARENT_CTX_MODE), intfSet));
			fail();
		} catch (VmsDeviceException.INTERFACE_UNAVAILABLE_FOR_CONTEXT e) {
			assertTrue(!e.getUnavailableInterfaces().isEmpty());
		}

	}

	@Test
	public void updateContextWithoutInterfacesTest() {
		createSeedData();
		Set<Long> intfSet = Sets.newConcurrentHashSet();
		assertFalse(delegate.getAllocatedInterfacesForContext(ROUTED_CTX_ID_1).isEmpty());
		assertFalse(delegate.getAllocatedInterfacesForContext(TRANSPARENT_ID_1).isEmpty());
		delegate.onUpdateContext(
				getVirtualContextPolicy(ROUTED_CTX_ID_1, getContextModeValue(ROUTED_CTX_MODE), intfSet));
		assertNull(delegate.getAllocatedInterfacesForContext(ROUTED_CTX_ID_1));
		delegate.onUpdateContext(
				getVirtualContextPolicy(TRANSPARENT_ID_1, getContextModeValue(TRANSPARENT_CTX_MODE), intfSet));
		assertNull(delegate.getAllocatedInterfacesForContext(TRANSPARENT_ID_1));

	}

	@Test
	public void deleteRoutedContextTest() {
		createSeedData();
		int allAvailableIntfForRoutedCtxPreDelete = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(ROUTED_CTX_MODE).size();
		int allAvailableIntfForTransparentCtx = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(TRANSPARENT_CTX_MODE).size();

		int allIntf = allIntfSet.size();

		int actual = allIntf - delegate.getAllUnavailableIntferfacesForMode(ROUTED_CTX_ID_1, ROUTED_CTX_MODE).size();
		assertEquals(allAvailableIntfForRoutedCtxPreDelete - 2, actual);

		delegate.onDeleteContext(ROUTED_CTX_ID_1);

		actual = allIntf - delegate.getAllUnavailableIntferfacesForMode(ROUTED_CTX_MODE).size();
		assertEquals(allAvailableIntfForRoutedCtxPreDelete, actual);

		actual = allIntf - delegate.getAllUnavailableIntferfacesForMode(TRANSPARENT_CTX_MODE).size();
		assertEquals(allAvailableIntfForTransparentCtx + 2, actual);
	}

	@Test
	public void deleteTransparentContextTest() {
		createSeedData();
		int allAvailableIntfForRoutedCtx = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(ROUTED_CTX_MODE).size();
		int allAvailableIntfForTransparentCtx = allIntfSet.size()
				- delegate.getAllUnavailableIntferfacesForMode(TRANSPARENT_CTX_MODE).size();

		int allIntf = allIntfSet.size();

		int actual = allIntf
				- delegate.getAllUnavailableIntferfacesForMode(TRANSPARENT_ID_1, TRANSPARENT_CTX_MODE).size();
		assertEquals(allAvailableIntfForRoutedCtx - 3, actual);

		actual = allIntf - delegate.getAllUnavailableIntferfacesForMode(ROUTED_CTX_ID_1, ROUTED_CTX_MODE).size();
		assertEquals(allAvailableIntfForRoutedCtx - 2, actual);

		delegate.onDeleteContext(TRANSPARENT_ID_1);

		actual = allIntf - delegate.getAllUnavailableIntferfacesForMode(ROUTED_CTX_MODE).size();
		assertEquals(allAvailableIntfForRoutedCtx + 3, actual);

		actual = allIntf - delegate.getAllUnavailableIntferfacesForMode(TRANSPARENT_CTX_MODE).size();
		assertEquals(allAvailableIntfForTransparentCtx + 3, actual);
	}

	@Test
	public void deleteContextNegativeTest() {
		createSeedData();
		delegate.onDeleteContext(999999l);
	}

	private String getContextModeValue(FwOSMode mode) {
		if (mode.equals(FwOSMode.OS_MODE_ROUTED)) {
			return "Routed";
		} else if (mode.equals(FwOSMode.OS_MODE_TRANSPARENT)) {
			return "Transparent";
		} else {
			return null;
		}
	}

	@After
	public void after() {
		delegate = null;
	}
}
