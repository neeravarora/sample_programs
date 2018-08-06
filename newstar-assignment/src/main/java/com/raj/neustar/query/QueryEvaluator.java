package com.raj.neustar.query;

import com.raj.neustar.dto.Query;
import com.raj.neustar.dto.Response;

/**
 * @author kumargau
 *
 * @param <T>
 */
public interface QueryEvaluator<T extends Object> {
	 Response<T> evalute(Query query, Class<T> clazz, Object ...params);

}
