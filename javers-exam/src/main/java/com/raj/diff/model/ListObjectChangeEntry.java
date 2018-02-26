package com.raj.diff.model;

import java.util.Comparator;
import java.util.TreeSet;

public class ListObjectChangeEntry extends PropertyChangeEntry{
	
	private TreeSet<ElementObjectChangeEntry> list;

	public ListObjectChangeEntry(String propertyName, Comparator<ElementObjectChangeEntry> comparator){
		super(propertyName);
		if(comparator != null)
		list = new TreeSet<ElementObjectChangeEntry>(comparator);
	}
	
	
}
