package com.raj.diff.model;

public class PropertyChangeEntry extends ChangeEntry{
	
	private final String propertyName;
	
	public PropertyChangeEntry(String propertyName){
		this.propertyName = propertyName;
	 }

	public String getPropertyName() {
		return propertyName;
	}

}
