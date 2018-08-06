package com.raj.neustar.dto;

import java.util.List;

import com.raj.neustar.constants.QueryType;

/**
 * @author kumargau
 *
 */
public interface Query {
	
	QueryType getQueryType();

	List<Object> getParameters();
}
