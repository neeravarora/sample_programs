package com.raj.diff.model;

import java.util.Map;

import com.google.common.collect.Maps;

public class KeyObjectChangeEntry<T> extends ChangeEntry{
	
private final Map<String, PropertyChangeEntry> keyPropertyChangeMap;
	
    //private final ElementChangeStatusEntry elementChangeStatusEntry;

    private final T id;
	
	public KeyObjectChangeEntry(T id) {
		this.id = id;
		this.keyPropertyChangeMap = Maps.newHashMap();
		
	}
	
	public T getId() {
		return id;
	}

	public void addKeyPropertyChangeEntry(String propertyName, PropertyChangeEntry propertyChangeEntry){
		keyPropertyChangeMap.put(propertyName, propertyChangeEntry);
	}

	public Map<String, PropertyChangeEntry> getKeyPropertyChangeMap() {
		return keyPropertyChangeMap;
	}

}
