package com.raj.neustar.algo;

/**
 * @author kumargau
 *
 */
public class PercentDiscountStrategy implements Strategy<Double, Double> {

	private Double percentValue;
	
	public PercentDiscountStrategy(Double percentValue) {
		this.percentValue = percentValue;
	}
	
	@Override
	public Double execute(Double price) {
		return price*(100-percentValue)/100;
	}
}
