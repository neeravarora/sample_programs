package com.raj.neustar.service;

import static org.junit.jupiter.api.Assertions.fail;

import org.junit.Before;
import org.junit.jupiter.api.Test;

import com.raj.neustar.query.QueryEngine;
import com.raj.neustar.repository.InventoryRepository;
import com.raj.neustar.repository.InventoryRepositoryImpl;

class InventoryServiceTest {
	
	private InventoryService inventoryService;
	
	@Before
	void setup() {
		InventoryRepository inventoryRepository = new InventoryRepositoryImpl(); 
		QueryEngine queryEngine = new QueryEngine();
		inventoryService = new InventoryServiceImpl(inventoryRepository, queryEngine);
	}

	@Test
	void test() {
		fail("Not yet implemented");
	}

}
