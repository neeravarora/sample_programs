package com.raj.neustar.constants;

import java.util.HashMap;
import java.util.Map;


/**
 * @author kumargau
 *
 */
public enum QueryType {
	PERCENT_DISCOUNT("1"), FLAT_DISCOUNT("2"), MAX_DIS_UNDER_CATEGORY("3"), ADD_ITEM("4"), REMOVE_ITEM(
			"5"), INVALID_QUERY(null);

	private String queryCode;

	private static final Map<String, QueryType> lookup = new HashMap<>();

	QueryType(String queryCode) {
		this.queryCode = queryCode;
	}

	static {
		for (QueryType qt : QueryType.values()) {
			lookup.put(qt.queryCode, qt);
		}
	}

	public static QueryType getQueryType(String queryCode) {
		QueryType queryType;
		return (queryType = lookup.get(queryCode)) != null ? queryType : INVALID_QUERY;
	}
}
