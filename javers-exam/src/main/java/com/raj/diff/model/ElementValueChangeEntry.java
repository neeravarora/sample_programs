package com.raj.diff.model;

public class ElementValueChangeEntry<T> extends KeyChangeEntry<Integer>{
	
	private final Value<T> left;
	private final Value<T> right;
	
	private final ElementChangeStatusEntry elementChangeStatusEntry;
	
	public ElementValueChangeEntry(Integer leftIndex, Integer rightIndex, Value<T> left, Value<T> right) {
		super(leftIndex, rightIndex);
		this.right = right;
		this.left = left;
		if(leftIndex == rightIndex){
			if(right == null && left != null)
				elementChangeStatusEntry = ElementChangeStatusEntry.DELETED;
			else if(left == null && right != null)
				elementChangeStatusEntry = ElementChangeStatusEntry.NEW_INSERTED;
			else if(!left.equals(right))
				elementChangeStatusEntry = ElementChangeStatusEntry.MODIFIED;
			else
				elementChangeStatusEntry = ElementChangeStatusEntry.ILLGAL;
		}else{
			if(left.equals(right))
				elementChangeStatusEntry = ElementChangeStatusEntry.RE_ODERED;
			else
				elementChangeStatusEntry = ElementChangeStatusEntry.ILLGAL;
		}
	}

	public Integer getLeftKey() {
		return getLeftKey();
	}

	public Integer getRightKey() {
		return getRightKey();
	}

	public Value<T> getLeft() {
		return left;
	}

	public Value<T> getRight() {
		return right;
	}

	public ElementChangeStatusEntry getElementChangeStatusEntry() {
		return elementChangeStatusEntry;
	}

}
