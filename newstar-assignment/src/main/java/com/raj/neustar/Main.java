package com.raj.neustar;

import java.io.IOException;

import com.raj.neustar.dto.ProductInfo;
import com.raj.neustar.dto.Response;
import com.raj.neustar.query.QueryEngine;
import com.raj.neustar.repository.InventoryRepository;
import com.raj.neustar.repository.InventoryRepositoryImpl;
import com.raj.neustar.service.InventoryService;
import com.raj.neustar.service.InventoryServiceImpl;
import com.raj.neustar.util.FileUtil;

/**
 * @author kumargau
 *
 */
public class Main {

	public static void main(String[] args) throws IOException {


		InventoryRepository inventoryRepository = new InventoryRepositoryImpl();
		QueryEngine queryEngine = new QueryEngine();
		InventoryService inventoryService = new InventoryServiceImpl(inventoryRepository, queryEngine);

		inventoryService.load(".\\src\\main\\resources\\data.txt");

		FileUtil.readGivenNumsOfLines(".\\src\\main\\resources\\query.txt", line -> {
			if (line.charAt(0) == '3') {
				Response<ProductInfo> productInfoResponse = inventoryService.query(line, ProductInfo.class);
				System.out.println(productInfoResponse.getData());
			} else {
				Response<String> response = inventoryService.query(line, String.class);
				System.out.println(response.getData());
			}
			return true;

		});

	}
}
