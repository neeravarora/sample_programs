package com.raj.neustar.service;



import com.raj.neustar.algo.Strategy;
import com.raj.neustar.algo.StrategyFactory;
import com.raj.neustar.algo.StrategyFactoryImpl;
import com.raj.neustar.constants.MessageConstants;
import com.raj.neustar.constants.QueryType;
import com.raj.neustar.dto.ProductInfo;
import com.raj.neustar.dto.Query;
import com.raj.neustar.dto.QueryImpl;
import com.raj.neustar.dto.Response;
import com.raj.neustar.query.QueryEngine;
import com.raj.neustar.query.QueryEvaluator;
import com.raj.neustar.repository.InventoryRepository;
import com.raj.neustar.util.Utility;

/**
 * @author kumargau
 *
 */
public class InventoryServiceImpl implements InventoryService{
	
	private QueryEngine queryEngine;	
	
	private InventoryRepository inventoryRepository;
	
	private StrategyFactory strategyFactory = new StrategyFactoryImpl();
	
	
	public InventoryServiceImpl(InventoryRepository inventoryRepository, QueryEngine queryEngine) {
		this.inventoryRepository = inventoryRepository;
		this.queryEngine = queryEngine;
		this.queryEngine.put(QueryType.FLAT_DISCOUNT, flatDiscountEvalutor);
		this.queryEngine.put(QueryType.PERCENT_DISCOUNT, percentDiscountEvalutor);
		this.queryEngine.put(QueryType.MAX_DIS_UNDER_CATEGORY, maxDisUnderCategoryEvalutor);
		this.queryEngine.put(QueryType.ADD_ITEM, addItemEvalutor);
		this.queryEngine.put(QueryType.REMOVE_ITEM, removeItemEvalutor);
		
	}
	
	@Override
	public  <T> Response<T> query(String queryStr, Class<T> clazz) {
		Query query= new QueryImpl(queryStr);
		return queryEngine.execute(query, clazz);
	}
	
	@Override
	public void load(String file) {
		inventoryRepository.load(file);
	}

	private final QueryEvaluator<String> flatDiscountEvalutor = (query, clazz, params) -> {

		Strategy<Double, Double> strategy = strategyFactory
				.getFlatDisStrategy(Utility.convert((String) params[1], Double.class));
		boolean status = inventoryRepository.applyDiscount(Utility.convert((String) params[0], Integer.class),
				strategy);
		if (status)
			return new Response<String>(MessageConstants.DISCOUNT_APPLIED);
		else
			return new Response<String>(MessageConstants.DISCOUNT_NOT_APPLIED);

	};
	
	private final QueryEvaluator<String> percentDiscountEvalutor = (query, clazz, params) -> {

		Strategy<Double, Double> strategy = strategyFactory
				.getPercentageDisStrategy(Utility.convert((String) params[1], Double.class));
		boolean status = inventoryRepository.applyDiscount(Utility.convert((String) params[0], Integer.class),
				strategy);
		if (status)
			return new Response<String>(MessageConstants.DISCOUNT_APPLIED);
		else
			return new Response<String>(MessageConstants.DISCOUNT_NOT_APPLIED);

	};
	
	private final QueryEvaluator<ProductInfo> maxDisUnderCategoryEvalutor = (query, clazz, params) -> {
		ProductInfo productInfo = inventoryRepository
				.getMaxDiscountedItem(Utility.convert((String) params[0], Integer.class));
		return new Response<ProductInfo>(productInfo);

	};
	
	private final QueryEvaluator<String> addItemEvalutor = (query, clazz, params) -> {

		Integer itemAddedId =inventoryRepository.addCategoryOrProduct(
				Utility.convert((String)params[0], Integer.class), 
				Utility.convert((String)params[1], Integer.class), 
				(String)params[3],
				Utility.convert((String)params[2], Double.class));
		if (itemAddedId != null && itemAddedId > 0)
			return new Response<String>(MessageConstants.ITEM_ADDED);
		else
			return new Response<String>(MessageConstants.FAILED);

	};
	
	
	private final QueryEvaluator<String> removeItemEvalutor = (query, clazz, params) -> {

		boolean status = inventoryRepository
				.removeCategoryOrProduct(Utility.convert((String) params[0], Integer.class));
		if (status)
			return new Response<String>(MessageConstants.ITEM_REMOVED);
		else
			return new Response<String>(MessageConstants.FAILED);

	};

//	//@Override
//	public  <T extends Object> T query1(String query, Class<T> clazz) {
//		if (query == null || query.isEmpty())
//			throw new IllegalArgumentException("Invaild query");
//		String[] queryTokens = query.split(" ");
//		QueryType queryType = QueryType.getQueryType(queryTokens[0]);
//		Strategy strategy = null;
//		boolean status = false;
//		switch (queryType) {
//		case FLAT_DISCOUNT:
//			validateQueryLength(queryTokens, 3);
//			strategy = strategyFactory.getFlatDisStrategy(Utility.convert(queryTokens[2], Double.class));
//			status = inventoryRepository.applyDiscount(Utility.convert(queryTokens[1], Integer.class),strategy
//					);
//			return (T)getResponse((Object)MessageConstants.DISCOUNT_APPLIED, status, clazz);
//		case PERCENT_DISCOUNT:
//			validateQueryLength(queryTokens, 3);
//			boolean status1 = inventoryRepository.applyDiscount(Utility.convert(queryTokens[1], Integer.class),
//					strategyFactory.getPercentageDisStrategy(Utility.convert(queryTokens[2], Double.class)));
//			return getResponse((T)MessageConstants.DISCOUNT_APPLIED, status, clazz);
//		case MAX_DIS_UNDER_CATEGORY:
//			validateQueryLength(queryTokens, 2);
//			inventoryRepository.getMaxDiscountedItem(Utility.convert(queryTokens[1], Integer.class));
//			break;
//		case ADD_ITEM:
//			validateQueryLength(queryTokens, 2);
//			inventoryRepository.addCategoryOrProduct(
//					Utility.convert(queryTokens[1], Integer.class), 
//					Utility.convert(queryTokens[2], Integer.class), 
//					queryTokens[3], 
//					Utility.convert(queryTokens[4], Double.class));
//			break;
//		case REMOVE_ITEM:
//			validateQueryLength(queryTokens, 2);
//			inventoryRepository.removeCategoryOrProduct(Utility.convert(queryTokens[1], Integer.class));
//			break;
//		case INVALID_QUERY:
//			break;
//
//		}
//
//		return null;
//	}
//
//	private Object getResponse(Object result, boolean status, Class clazz) {
//		if(clazz.equals(String.class))
//		   return status  ? result : MessageConstants.FAILED ;
//		else 
//			throw new IllegalArgumentException("Invaild query");
//	}
//	
//	void validateQueryLength(String[] queryToken, int length) {
//		if(queryToken.length != length) {
//			throw new IllegalArgumentException("Invalid query parameters");
//		}
//	}

}
