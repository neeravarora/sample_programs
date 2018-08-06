package com.raj.neustar.dto;

import java.util.ArrayList;
import java.util.List;

import com.raj.neustar.constants.QueryType;

/**
 * @author kumargau
 *
 */
public class QueryImpl implements Query {

	private QueryType queryType;
	private List<Object> parameters;

	public QueryImpl(String query) {
		this(query, " ");
	}

	public QueryImpl(String query, String regx) {
		if (query != null && !query.isEmpty()) {
			String[] queryTokens = query.split(regx);
			this.queryType = QueryType.getQueryType(queryTokens[0]);
			this.parameters = new ArrayList<>();
			for (int i = 1; i < queryTokens.length; i++) {
				this.parameters.add(queryTokens[i]);
			}

		}
	}

	public QueryType getQueryType() {
		return queryType;
	}

	@SuppressWarnings("unchecked")
	public List<Object> getParameters() {
		return (ArrayList<Object>) ((ArrayList<Object>) parameters).clone();
	}

}
