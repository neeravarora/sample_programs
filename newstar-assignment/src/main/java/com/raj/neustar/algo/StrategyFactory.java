package com.raj.neustar.algo;

/**
 * @author kumargau
 *
 */
public interface StrategyFactory {
	
	Strategy<Double, Double> getFlatDisStrategy(Double disAmt);
	
	Strategy<Double, Double> getPercentageDisStrategy(Double disPercent);

}
