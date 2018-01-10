package com.raj.diff.model;

import java.util.Map;

import com.google.common.collect.Maps;

public class ElementObjectChangeEntry extends KeyChangeEntry<Integer>{
	private final Map<String, PropertyChangeEntry> propertyChangeMap;
	
    //private final ElementChangeStatusEntry elementChangeStatusEntry;
	
	public ElementObjectChangeEntry(Integer leftIndex, Integer rightIndex) {
		super(leftIndex, rightIndex);
		this.propertyChangeMap = Maps.newHashMap();
		
	}
	
	public void addPropertyChangeEntry(String propertyName, PropertyChangeEntry propertyChangeEntry){
		propertyChangeMap.put(propertyName, propertyChangeEntry);
	}

	public Map<String, PropertyChangeEntry> getPropertyChangeMap() {
		return propertyChangeMap;
	}

}
