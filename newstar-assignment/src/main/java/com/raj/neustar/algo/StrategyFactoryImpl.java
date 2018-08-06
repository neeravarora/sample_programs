package com.raj.neustar.algo;

/**
 * @author kumargau
 *
 */
public class StrategyFactoryImpl implements StrategyFactory{

	@Override
	public Strategy<Double, Double> getFlatDisStrategy(Double disAmt) {
		return new FlatDiscountStrategy(disAmt);
	}

	@Override
	public Strategy<Double, Double> getPercentageDisStrategy(Double disPercent) {
		return new PercentDiscountStrategy(disPercent);
	}

}
