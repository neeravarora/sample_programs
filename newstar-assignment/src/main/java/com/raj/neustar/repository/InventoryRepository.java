package com.raj.neustar.repository;

import com.raj.neustar.algo.Strategy;
import com.raj.neustar.dto.ProductInfo;

/**
 * @author kumargau
 *
 */
public interface InventoryRepository {

	boolean applyDiscount(Integer categoryOrProductId, Strategy<Double, Double> strategy);

	Integer addCategoryOrProduct(Integer parentId, Integer childId, String name, Double price);

	boolean removeCategoryOrProduct(Integer categoryOrProductId);

	ProductInfo getMaxDiscountedItem(Integer categoryId);

	void load(String file);

}
