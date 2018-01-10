package com.raj.diff.model;

public class KeyChangeEntry<T> extends ChangeEntry {
	private final T leftKey;
	private final T rightKey;

	public KeyChangeEntry(T leftIndex, T rightIndex) {
		this.leftKey = leftIndex;
		this.rightKey = rightIndex;
	}

	public T getLeftKey() {
		return leftKey;
	}

	public T getRightKey() {
		return rightKey;
	}
	
	
}
