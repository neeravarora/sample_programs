package com.raj;

import org.javers.core.metamodel.annotation.Id;

public class ID<T> {

	//@Id
	private T id;

	public T getId() {
		return id;
	}

	public void setId(T id) {
		this.id = id;
	}
	
}
