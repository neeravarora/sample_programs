package com.cisco.nm.vms.fw.policy.controller;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.cisco.nm.vms.fw.policy.controller.BidirectionalMap.Entry;

/**
 * 
 * This is a utility to store many to many bidirectional associations between any type of entities.
 * In a traditional Map, only one to one associations can be stored and queried, 
 * e.g each value can be mapped to a key, and key can be used to query the value associated with it.
 * 
 * In this bidirectional Map implementation, many to many bidirectional associations can be stored and queried, 
 * e.g. Multiple second entity objects can be mapped to a first entity object, the first entity object can be used to query the second entity object, 
 * similarly the bidirectional association from second entity object to first entity objects can also be queried.
 *
 * Associations entries with null values are not allowed.
 *
 * BiDirectionalMap<> biDirectMap = new BiDirectionalMap<String,String>();
 *
 * biDirectMap.put("Student1","Course1");
 * biDirectMap.put("Student1","Course2");
 * biDirectMap.put("Student1","Course3");
 * biDirectMap.put("Student2","Course2");
 * biDirectMap.put("Student2","Course3");
 *
 * biDirectMap.get("Course1"); // returns Student1
 * biDirectMap.get("Course2"); // returns Student1 and Student2
 * biDirectMap.get("Student1"); // returns Course1, Course2 and Course3
 *  
 * @author rgauttam
 *
 * @param <F> the type of first entities maintained by this map
 * @param <S> the type of associated second entities maintained by this map
 * 
 * @see     com.google.common.collect.Maps
 * @see     com.google.common.collect.Sets
 * @see     java.util.Iterator
 * @see     java.util.Map
 * @see     java.util.Set;
 * 
 * 
 */
public class BidirectionalMap<F, S> implements Iterable<Entry<F, S>> {

	/**
	 * This map keeps one to many association between 
	 * every first entity object to one or many second entity objects.
	 */
	private final Map<F, Set<S>> firstToSecondMap = Maps.newConcurrentMap();

	/**
	 * This map keeps one to many association between 
	 * every second entity object to one or many first entity objects.
	 */
	private final Map<S, Set<F>> secondToFirstMap = Maps.newConcurrentMap();

	/**
	 * It returns a new instance of BidirectionalMap.
	 * 
	 * @return
	 */
	public static <F, S> BidirectionalMap<F, S> newBidirectionalMap() {
		return new BidirectionalMap<>();
	}

	/**
	 * One to one association can be created using this method.
	 * It adds an one to one association entry between first and second entity.
	 * 
	 * @param first is an object of first <F> entity type 
	 * @param second is an object of second <S> entity type 
	 * @throws IllegalArgumentException if any of the parameter is null.
	 */
	public void put(F first, S second) {
		validateArgs(first, second);
		putInMap(firstToSecondMap, first, second);
		putInMap(secondToFirstMap, second, first);
	}

	/**
	 * One to many association can be created using this method.
	 * It adds an one to many association entries between first and set of second entity.
	 * 
	 * @param first is an object of first <F> entity type 
	 * @param secondSet is a set of second <S> entity type.
	 * @throws IllegalArgumentException if any of parameter is null or secondSet is empty..
	 */
	public void putAll(F first, Set<S> secondSet) {
		putInMap(firstToSecondMap, secondToFirstMap, first, secondSet);
	}

	/**
	 * Many to one association can be created using this method.
	 * It adds an many to one association entries between set of first and second entity.
	 * 
	 * @param firstSet is a set of first <F> entity type.
	 * @param second is an object of second <S> entity type.
	 * @throws IllegalArgumentException if any of parameter is null or firstSet is empty.
	 */
	public void putAll(Set<F> firstSet, S second) {
		putInMap(secondToFirstMap, firstToSecondMap, second, firstSet);
	}

	private <T, U> void putInMap(Map<T, Set<U>> map1, Map<U, Set<T>> map2, T key, Set<U> valueSet) {
		validateArgs(key, valueSet);
		putInMap(map1, key, valueSet);
		valueSet.forEach(value -> putInMap(map2, value, key));
	}

	private <T, U> void putInMap(Map<T, Set<U>> map, T t, U u) {
		if (map.get(t) == null) {
			map.put(t, Sets.newConcurrentHashSet());
		}
		map.get(t).add(u);
	}

	private <T, U> void putInMap(Map<T, Set<U>> map, T t, Set<U> set) {
		if (map.get(t) == null) {
			map.put(t, Sets.newConcurrentHashSet());
		}
		map.get(t).addAll(set);
	}

	/**
	 * @return all available associated set of first <F> entity objects from this bidirectional map. 
	 */
	public Set<F> getFirstKeySet() {
		return firstToSecondMap.keySet();
	}

	/**
	 * @return all available associated set of second <S> entity objects from this bidirectional map. 
	 */
	public Set<S> getSecondKeySet() {
		return secondToFirstMap.keySet();
	}

	/**
	 * @param first
	 * @return all set of second entities which are associated with first.
	 */
	public Set<S> getByFirst(F first) {
		return firstToSecondMap.get(first);
	}

	/**
	 * @param second
	 * @return all set of first entities which are associated with second.
	 */
	public Set<F> getBySecond(S second) {
		return secondToFirstMap.get(second);
	}

	/**
	 * Remove association entries between first and its associated set of seconds.
	 * 
	 * @param first
	 * @return all set of second entities which were associated with first.
	 */
	public Set<S> removeByFirst(F first) {
		return remove(firstToSecondMap, secondToFirstMap, first);
	}

	/**
	 *  Remove association entries between second and its associated set of firsts.
	 * 
	 * @param second
	 * @return all set of first entities which are associated with second.
	 */
	public Set<F> removeBySecond(S second) {
		return remove(secondToFirstMap, firstToSecondMap, second);
	}

	private <T, U> Set<U> remove(Map<T, Set<U>> map1, Map<U, Set<T>> map2, T key) {
		Set<U> itemsToRemove = null;
		if (key == null || (itemsToRemove = map1.remove(key)) == null)
			return null;
		itemsToRemove.forEach(item -> {
			Set<T> keySetAssociatedToItem = map2.get(item);
			if (keySetAssociatedToItem != null) {
				keySetAssociatedToItem.remove(key);
				if (keySetAssociatedToItem.isEmpty()) {
					map2.remove(item);
				}
			}
		});

		return itemsToRemove;
	}

	/**
	 * It checks that is there any association exists between first and second.
	 * 
	 * @param first
	 * @param second
	 * @return
	 */
	public boolean isEntryExist(F first, S second) {
		Set<S> secondSet = getByFirst(first);
		return secondSet != null && secondSet.contains(second);
	}

	/**
	 * It returns all set of orphaned first entity value.
	 * TODO currently no use of this. we are not allowing orphaned values here.
	 *      We need to add flags to either allow or not allow orphaned  
	 *  
	 * @return
	 */
	public Set<F> getDisJointSubSetOfFirst() {
		return getDisJointSubSet(firstToSecondMap);
	}

	/**
	 * It returns all set of orphaned second entity value.
	 * TODO currently no use of this. we are not allowing orphaned values here.
	 *      We need to add flags to either allow or not allow orphaned  
	 *  
	 * @return
	 */
	public Set<S> getDisJointSubSetOfSecond() {
		return getDisJointSubSet(secondToFirstMap);
	}

	private <T, U> Set<T> getDisJointSubSet(Map<T, Set<U>> map) {
		Set<T> disJointKeySubSetOfMap = Sets.newConcurrentHashSet();
		map.keySet().forEach(key -> {
			if (map.get(key) == null || map.get(key).isEmpty())
				disJointKeySubSetOfMap.add(key);
		});
		return disJointKeySubSetOfMap;
	}

	/**
	 * Remove the association between first and second.
	 * 
	 * @param first
	 * @param second
	 * @return true, if it finds any association and removes successfully else false.
	 */
	public boolean removeEntry(F first, S second) {
		return removeEntry(firstToSecondMap, secondToFirstMap, first, second);
	}

	private <T, U> boolean removeEntry(Map<T, Set<U>> map1, Map<U, Set<T>> map2, T first, U second) {
		if (first == null || second == null)
			return false;
		boolean result = true;
		Set<U> secondSet = map1.get(first);
		if (secondSet != null) {
			if (!secondSet.isEmpty())
				result = result && secondSet.remove(second);
			if (secondSet.isEmpty())
				map1.remove(first);
		}

		Set<T> firstSet = map2.get(second);
		if (firstSet != null) {
			if (!firstSet.isEmpty())
				result = result && firstSet.remove(first);
			if (firstSet.isEmpty())
				map2.remove(second);
		}

		return result;
	}

	/**
	 * Removes all the entries and makes it empty.
	 */
	public void clear() {
		firstToSecondMap.clear();
		secondToFirstMap.clear();
	}

	/* 
	 * It will construct and return a string from all entries of this map.
	 * 
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("[");
		this.forEach(e -> {
			sb.append("{");
			sb.append(e.getFirst());
			sb.append(", ");
			sb.append(e.getSecond());
			sb.append("}");
			sb.append(", ");
		});
		sb.delete(sb.length() - 2, sb.length());
		sb.append("]");
		return sb.toString();
	}

	@SuppressWarnings("rawtypes")
	private <T, U> void validateArgs(T t, U u) {
		if (t == null || u == null)
			throw new IllegalArgumentException("Null Values are not allowed");
		else if (t instanceof Set) {
			if (((Set) t).isEmpty())
				throw new IllegalArgumentException("Empty set is not allowed");
		} else if (u instanceof Set) {
			if (((Set) u).isEmpty())
				throw new IllegalArgumentException("Empty set is not allowed");
		}
	}

	/* 
	 * It returns a iterator which can iterate whole map in terms of entries.
	 * Above entries will be grouped by first entity.
	 *    
	 * 
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	public Iterator<Entry<F, S>> iterator() {
		return new Iterator<Entry<F, S>>() {

			private Entry<F, S> current = null;
			private Entry<F, S> next = null;

			{
				next = new Entry<F, S>(null, null);
				Entry<F, S> previous = next;
				for (java.util.Map.Entry<F, Set<S>> entry : firstToSecondMap.entrySet()) {
					Set<S> values = entry.getValue();
					for (S value : values) {
						previous.setNext(new Entry<F, S>(entry.getKey(), value));
						previous = previous.getNext();
					}
				}
				next = next.getNext();
			}

			@Override
			public boolean hasNext() {
				return next != null;
			}

			@Override
			public Entry<F, S> next() {
				current = next;
				next = current.getNext();
				return current;
			}

		};
	}

	/**
	 * It is an association entry exists in this bidirectional map.
	 * 
	 * @author rgauttam
	 *
	 * @param <F>
	 * @param <S>
	 */
	public static class Entry<F, S> {
		private F first;
		private S second;
		private Entry<F, S> next;

		Entry(F first, S second, Entry<F, S> next) {
			this.first = first;
			this.second = second;
			this.next = next;
		}

		Entry(F first, S second) {
			this.first = first;
			this.second = second;
		}

		public F getFirst() {
			return first;
		}

		public void setFirst(F first) {
			this.first = first;
		}

		public S getSecond() {
			return second;
		}

		public void setSecond(S second) {
			this.second = second;
		}

		Entry<F, S> getNext() {
			return next;
		}

		void setNext(Entry<F, S> next) {
			this.next = next;
		}
	}
}