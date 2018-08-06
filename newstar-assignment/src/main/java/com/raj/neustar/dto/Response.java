package com.raj.neustar.dto;

/**
 * @author kumargau
 *
 * @param <T>
 */
public class Response<T extends Object> {

	private T data;

	public Response(T data) {
		super();
		this.data = data;
	}

	public T getData() {
		return data;
	}
	
}
