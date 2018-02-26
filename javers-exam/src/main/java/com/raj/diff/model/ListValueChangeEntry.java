package com.raj.diff.model;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

public class ListValueChangeEntry<T> extends PropertyChangeEntry{
	
	private TreeSet<ElementValueChangeEntry<T>> list;

	public ListValueChangeEntry(String propertyName, Comparator<ElementValueChangeEntry> comparator){
		super(propertyName);
		if(comparator != null)
		list = new TreeSet<ElementValueChangeEntry<T>>(comparator);
	}
	
	
}
