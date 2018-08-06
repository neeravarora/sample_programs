package com.raj.neustar.algo;

/**
 * @author kumargau
 *
 * @param <T>
 * @param <U>
 */
public interface Strategy<T, U> {
	
	 T execute(U u);

}
