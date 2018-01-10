package com.raj.diff.model;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

public class ListChangeEntry<T> extends PropertyChangeEntry{
	
	private TreeSet<ElementValueChangeEntry<T>> list;

	public ListChangeEntry(String propertyName, Comparator<ElementValueChangeEntry> comparator){
		super(propertyName);
		list = new TreeSet<ElementValueChangeEntry<T>>(comparator);
	}
	
	
}
