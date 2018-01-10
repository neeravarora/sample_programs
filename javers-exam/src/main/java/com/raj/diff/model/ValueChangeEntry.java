package com.raj.diff.model;

public class ValueChangeEntry<T> extends PropertyChangeEntry {

	private final Value<T> left;
	private final Value<T> right;

	public ValueChangeEntry(String propertyName, Value<T> left, Value<T> right) {
		super(propertyName);
		this.right = right;
		this.left = left;
	}

	public Value<T> getLeft() {
		return left;
	}

	public Value<T> getRight() {
		return right;
	}

}
