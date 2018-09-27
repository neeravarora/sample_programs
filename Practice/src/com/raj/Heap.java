package com.raj;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

public class Heap<T> {

	List<T> list = new LinkedList();
	Comparator<T> comparator;
	int size = 0;
	
	public Heap(){
		
	}
	
    public Heap(Comparator<T> comparator){
		
	}
}
