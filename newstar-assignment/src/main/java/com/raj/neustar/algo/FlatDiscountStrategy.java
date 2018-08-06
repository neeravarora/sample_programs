package com.raj.neustar.algo;


/**
 * @author kumargau
 *
 */
public class FlatDiscountStrategy implements Strategy<Double, Double> {
	
	private Double flatValue;
	
	public FlatDiscountStrategy(Double flatValue) {
		this.flatValue = flatValue;
	}

	@Override
	public Double execute(Double origional) {
		return origional - flatValue;
	}

}