package com.raj.neustar.query;

import java.util.HashMap;
import java.util.Map;

import com.raj.neustar.constants.QueryType;
import com.raj.neustar.dto.Query;
import com.raj.neustar.dto.Response;

/**
 * @author kumargau
 *
 */
@SuppressWarnings("rawtypes")
public class QueryEngine {

	private Map<QueryType, QueryEvaluator> queryMap;

	public QueryEngine() {
		queryMap = new HashMap<>();
	}

	@SuppressWarnings("unchecked")
	public <T> Response<T> execute(Query query, Class<T> clazz) {
		return queryMap.get(query.getQueryType()).evalute(query, clazz, query.getParameters().toArray());
	}

	public <T> void put(QueryType queryType, QueryEvaluator<T> queryEvalutor) {
		queryMap.put(queryType, queryEvalutor);
	}

	public void remove(QueryType queryType) {
		queryMap.remove(queryType);
	}

}
