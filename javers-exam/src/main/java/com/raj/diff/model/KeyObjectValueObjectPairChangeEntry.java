package com.raj.diff.model;

import java.util.Map;

import com.google.common.collect.Maps;

public class KeyObjectValueObjectPairChangeEntry<T> extends KeyObjectChangeEntry<T>{
	
	private final Map<String, PropertyChangeEntry> valueObjectPropertyChangeMap;

	public KeyObjectValueObjectPairChangeEntry(T id) {
		super(id);
		this.valueObjectPropertyChangeMap = Maps.newHashMap();
	}
	
	public void addKeyObjectPropertyChangeEntry(String propertyName, PropertyChangeEntry propertyChangeEntry){
		valueObjectPropertyChangeMap.put(propertyName, propertyChangeEntry);
	}

	public Map<String, PropertyChangeEntry> getKeyObjcetPropertyChangeMap() {
		return valueObjectPropertyChangeMap;
	}

}
