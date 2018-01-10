package com.raj.diff;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

public class SingleValueObjects {

	@SuppressWarnings("rawtypes")
	private final Set<Class> singleValueObjectTypes = new HashSet<>();

	{
		addSingleValueObjectTpye(Integer.class).
		addSingleValueObjectTpye(Long.class).
		addSingleValueObjectTpye(Short.class).
		addSingleValueObjectTpye(Byte.class).
		addSingleValueObjectTpye(BigInteger.class).
		addSingleValueObjectTpye(Float.class).
		addSingleValueObjectTpye(Double.class).
		addSingleValueObjectTpye(BigDecimal.class).
		addSingleValueObjectTpye(String.class);
	}
	
	
	public <T>SingleValueObjects addSingleValueObjectTpye(Class<T> clazz){
		singleValueObjectTypes.add(clazz);
		return this;
	}
	
	public <T>SingleValueObjects clear(){
		singleValueObjectTypes.clear();
		return this;
	}
	
	public <T> Boolean isSingleValueObjectTpye(Class<T> clazz){
		return singleValueObjectTypes.contains(clazz);
	}

}
