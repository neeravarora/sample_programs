package com.raj.neustar.service;

import com.raj.neustar.dto.Response;

/**
 * @author kumargau
 *
 */
public interface InventoryService {

	<T> Response<T> query(String query, Class<T> clazz);

	void load(String file);
}
