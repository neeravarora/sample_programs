package com.cisco.nm.vms.fw.policy.controller;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import com.cisco.nm.vms.fw.policy.controller.BidirectionalMap.Entry;
import com.google.common.collect.Sets;

@RunWith(PowerMockRunner.class)
public class BidirectionalMapTest {

	BidirectionalMap<Integer, Integer> map = null;

	@Before
	public void setUp() {
		map = BidirectionalMap.newBidirectionalMap();
	}

	private void createSeedData() {
		map.clear();
		map.put(1, 101);
		map.put(1, 201);
		map.put(1, 301);
		map.put(2, 102);
		map.put(2, 202);
		map.put(3, 103);
		map.put(601, 6);
		map.put(602, 6);
	}

	@Test
	public void createOne2OneEntryTest() {
		createSeedData();
		map.put(1, 401);
		assertEquals(4, map.getByFirst(1).size());
	}

	@Test
	public void createOne2OneEntryWithInvalidTest() {
		try {
			map.put(null, null);
		} catch (IllegalArgumentException e) {
			assertTrue(true);
		}
		try {
			map.put(10, null);
		} catch (IllegalArgumentException e) {
			assertTrue(true);
		}
		try {
			map.put(null, 10);
		} catch (IllegalArgumentException e) {
			assertTrue(true);
		}
	}

	@Test
	public void createOne2OneDuplicateEntryTest() {
		createSeedData();
		map.put(1, 401);
		map.put(1, 401);
		assertEquals(4, map.getByFirst(1).size());
	}

	@Test
	public void createMany2OneEntryTest() {
		createSeedData();
		Set<Integer> seconds = Sets.newConcurrentHashSet();
		seconds.add(701);
		seconds.add(702);
		map.putAll(7, seconds);
		assertEquals(2, map.getByFirst(7).size());
	}

	@Test
	public void createMany2OneEntryWithInvalidTest() {
		try {
			map.putAll(null, 10);
		} catch (IllegalArgumentException e) {
			assertTrue(true);
		}
		try {
			map.putAll(Sets.newHashSet(), null);
		} catch (IllegalArgumentException e) {
			assertTrue(true);
		}
	}

	@Test
	public void createOne2ManyEntryTest() {
		createSeedData();
		Set<Integer> firsts = Sets.newConcurrentHashSet();
		firsts.add(7);
		firsts.add(8);
		firsts.add(9);
		map.putAll(firsts, 1000);
		assertNotNull(map.getBySecond(1000));
		assertEquals(3, map.getBySecond(1000).size());
	}

	@Test
	public void createOne2ManyEntryWithInvalidTest() {
		try {
			map.putAll(10, null);
		} catch (IllegalArgumentException e) {
			assertTrue(true);
		}
		try {
			map.putAll(null, Sets.newHashSet());
		} catch (IllegalArgumentException e) {
			assertTrue(true);
		}
	}

	@Test
	public void getFirstKeySetTest() {
		createSeedData();
		assertNotNull(map.getFirstKeySet());
		assertTrue(map.getFirstKeySet().size() > 0);
	}

	@Test
	public void getSecondKeySetTest() {
		createSeedData();
		assertNotNull(map.getSecondKeySet());
		assertTrue(map.getSecondKeySet().size() > 0);
	}

	@Test
	public void getByFirstTest() {
		createSeedData();
		assertNotNull(map.getByFirst(1));
		assertEquals(3, map.getByFirst(1).size());
	}

	@Test
	public void getBySecondTest() {
		createSeedData();
		assertNotNull(map.getBySecond(6));
		assertEquals(2, map.getBySecond(6).size());
	}

	@Test
	public void removeByFirstTest() {
		createSeedData();
		assertEquals(3, map.getByFirst(1).size());
		assertEquals(3, map.removeByFirst(1).size());
		assertNull(map.getByFirst(1));
	}

	@Test
	public void removeByFirstWithInvalidValueTest() {
		createSeedData();
		assertNull(map.removeByFirst(9875675));
		assertNull(map.getByFirst(9875675));
	}

	@Test
	public void removeBySecondTest() {
		createSeedData();
		assertEquals(2, map.getBySecond(6).size());
		assertEquals(2, map.removeBySecond(6).size());
		assertNull(map.getBySecond(6));
	}

	@Test
	public void removeBySecondWithInvalidValueTest() {
		createSeedData();
		assertNull(map.removeBySecond(9875675));
		assertNull(map.getBySecond(9875675));
	}

	@Test
	public void isEntryExistTest() {
		createSeedData();
		assertTrue(map.isEntryExist(1, 101));
		assertFalse(map.isEntryExist(9, 101));
		assertFalse(map.isEntryExist(1, 900));
	}

	@Test
	public void getDisJointSubSetOfFirstTest() {
	}

	@Test
	public void getDisJointSubSetOfSecondTest() {
	}

	@Test
	public void removeEntryTest() {
		createSeedData();
		assertTrue(map.isEntryExist(1, 101));
		assertTrue(map.isEntryExist(1, 201));
		assertTrue(map.isEntryExist(1, 301));
		assertTrue(map.removeEntry(1, 101));
		assertTrue(map.removeEntry(1, 201));
		assertTrue(map.removeEntry(1, 301));
		assertNull(map.getByFirst(1));
		assertNull(map.getBySecond(101));
		assertNull(map.getBySecond(201));
		assertNull(map.getBySecond(301));
		assertFalse(map.isEntryExist(1, 101));
		assertFalse(map.isEntryExist(1, 201));
		assertFalse(map.isEntryExist(1, 301));
	}

	@Test
	public void removeInvalidEntryTest() {
		createSeedData();
		assertTrue(map.removeEntry(1, 101));
		assertTrue(map.removeEntry(1, 201));
		assertTrue(map.removeEntry(1, 301));
		assertTrue(map.removeEntry(6, 601));
		assertFalse(map.removeEntry(601, null));
		assertFalse(map.removeEntry(null, 6));
		assertFalse(map.removeEntry(null, null));
		assertFalse(map.removeEntry(1000, null));
		assertFalse(map.removeEntry(null, 1000));
		assertNull(map.getByFirst(1));
		assertTrue(map.getFirstKeySet().contains(601));
		assertTrue(map.getSecondKeySet().contains(6));

	}

	@Test
	public void clearTest() {
		createSeedData();
		assertFalse(map.getFirstKeySet().isEmpty());
		assertFalse(map.getSecondKeySet().isEmpty());
		map.clear();
		assertTrue(map.getFirstKeySet().isEmpty());
		assertTrue(map.getSecondKeySet().isEmpty());
	}

	@Test
	public void iteratorTest() {
		createSeedData();
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		sb.append("\n");
		for (Entry<Integer, Integer> entry : map) {
			map.put(7, 7);
			sb.append(" {");
			sb.append("First Value: ");
			sb.append(entry.getFirst());
			sb.append(", ");
			sb.append("Second Value: ");
			sb.append(entry.getSecond());
			sb.append("}");
			sb.append("\n");
		}
		sb.append("]");
		sb.append("\n");

		StringBuilder expectedSb = new StringBuilder();
		expectedSb.append("[");
		expectedSb.append("\n");
		expectedSb.append(" {First Value: 1, Second Value: 101}\n");
		expectedSb.append(" {First Value: 1, Second Value: 201}\n");
		expectedSb.append(" {First Value: 1, Second Value: 301}\n");
		expectedSb.append(" {First Value: 2, Second Value: 102}\n");
		expectedSb.append(" {First Value: 2, Second Value: 202}\n");
		expectedSb.append(" {First Value: 3, Second Value: 103}\n");
		expectedSb.append(" {First Value: 601, Second Value: 6}\n");
		expectedSb.append(" {First Value: 602, Second Value: 6}\n");
		expectedSb.append("]");
		expectedSb.append("\n");
		assertEquals(expectedSb.toString(), sb.toString());

		sb = new StringBuilder();
		sb.append("[");
		sb.append("\n");
		for (Entry<Integer, Integer> entry : map) {
			map.put(7, 7);
			sb.append(" {");
			sb.append("First Value: ");
			sb.append(entry.getFirst());
			sb.append(", ");
			sb.append("Second Value: ");
			sb.append(entry.getSecond());
			sb.append("}");
			sb.append("\n");
		}
		sb.append("]");
		sb.append("\n");

		expectedSb = new StringBuilder();
		expectedSb.append("[");
		expectedSb.append("\n");
		expectedSb.append(" {First Value: 1, Second Value: 101}\n");
		expectedSb.append(" {First Value: 1, Second Value: 201}\n");
		expectedSb.append(" {First Value: 1, Second Value: 301}\n");
		expectedSb.append(" {First Value: 2, Second Value: 102}\n");
		expectedSb.append(" {First Value: 2, Second Value: 202}\n");
		expectedSb.append(" {First Value: 3, Second Value: 103}\n");
		expectedSb.append(" {First Value: 7, Second Value: 7}\n");
		expectedSb.append(" {First Value: 601, Second Value: 6}\n");
		expectedSb.append(" {First Value: 602, Second Value: 6}\n");
		expectedSb.append("]");
		expectedSb.append("\n");
		assertEquals(expectedSb.toString(), sb.toString());
	}

	@Test
	public void toStringTest() {
		createSeedData();
		String expected = "[{1, 101}, {1, 201}, {1, 301}, {2, 102}, {2, 202}, {3, 103}, {601, 6}, {602, 6}]";
		assertEquals(expected, map.toString());
	}

	@After
	public void after() {
		map.clear();
	}
}
