package com.raj.neustar.repository;

import com.raj.neustar.algo.Strategy;
import com.raj.neustar.dto.ProductInfo;

/**
 * @author kumargau
 *
 */
public interface InventoryRepository {

	boolean applyDiscount(Integer convert, Strategy<Double, Double> flatDisStrategy);

	Integer addCategoryOrProduct(Integer convert, Integer convert2, String string, Double convert3);

	boolean removeCategoryOrProduct(Integer convert);

	ProductInfo getMaxDiscountedItem(Integer categoryId);

	void load(String file);

}
