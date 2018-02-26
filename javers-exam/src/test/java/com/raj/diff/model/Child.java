package com.raj.diff.model;

import org.javers.core.metamodel.annotation.Id;

public class Child {
	
	 @Id
	 Integer id;
	 String name;
	public Child(Integer id, String name) {
		super();
		this.id = id;
		this.name = name;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	 
	 

	

}
