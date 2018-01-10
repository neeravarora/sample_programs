package com.raj.diff.model;

import java.util.Map;

import com.google.common.collect.Maps;

public class ObjectChangeEntry extends PropertyChangeEntry{
	
	private final Map<String, PropertyChangeEntry> propertyChangeMap;
	
	public ObjectChangeEntry(String propertyName){
		super(propertyName);
		this.propertyChangeMap = Maps.newHashMap();
	}
	
	public void addPropertyChangeEntry(String propertyName, PropertyChangeEntry propertyChangeEntry){
		propertyChangeMap.put(propertyName, propertyChangeEntry);
	}

	public Map<String, PropertyChangeEntry> getPropertyChangeMap() {
		return propertyChangeMap;
	}
	
}
