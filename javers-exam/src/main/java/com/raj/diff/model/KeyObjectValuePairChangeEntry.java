package com.raj.diff.model;

public class KeyObjectValuePairChangeEntry<T, U>  extends KeyObjectChangeEntry<T>{
	
	private final Value<U> left;
	private final Value<U> right;

	public KeyObjectValuePairChangeEntry(T id, U left, U right) {
		super(id);
		this.left = new Value<U>(left);
		this.right = new Value<U>(right);
	}

}
