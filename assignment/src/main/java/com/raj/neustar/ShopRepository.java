package com.raj.neustar;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ShopRepository {

	private Map<Integer, Node> nodeCache = new ConcurrentHashMap<>();

	// public ShopRepository(String file){
	// this.root = new CategoryNode(1, "All Products");
	// // load(file);
	// }

	private Node root;
	
	public Map<Integer, Node> getCache(){
		return nodeCache;
	}

	static abstract class Node {

		private Integer id;
		private String name;

		public Node(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public abstract void setParent(Node root);

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

	}

	static class ProductNode extends Node {
		private double price;
		private Node parent;

		public ProductNode(Integer id, String name, double price) {
			super(id, name);
			this.price = price;
		}

		@Override
		public void setParent(Node parent) {
			if (parent instanceof CategoryNode) {
				((CategoryNode) parent).addChild(this);
			}
			this.parent = parent;
		}
		// getters

	}

	static class CategoryNode extends Node {
		private Set<Node> children;
		private Node parent;

		public CategoryNode(Integer id, String name) {
			super(id, name);
			this.children = new HashSet<>();

		}

		public Iterator<Node> getChildren() {
			return this.children.iterator();
		}

		public void addChild(Node node) {
			this.children.add(node);
		}

		public void removeChild(Node node) {
			this.children.remove(node);
		}

		public void clearChild() {
			this.children.clear();
		}

		@Override
		public void setParent(Node parent) {
			if (parent instanceof CategoryNode) {
				((CategoryNode) parent).addChild(this);
			}
			this.parent = parent;
		}

		// add, addAll, clear and remove for child in children
		// getters for others
		// setter for parent
	}

	public void load(String file) throws IOException {

		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		String line = br.readLine();
		Integer onOfNodes = convert(line, Integer.class);
		while ((line = br.readLine()) != null && onOfNodes-- > 0) {
			String[] tokens = line.split(" ");
			if (tokens.length != 3)
				throw new IllegalArgumentException("Invalid Input Data");

			Node node = null;
			if (tokens[1] == "-1") {
				node = new CategoryNode(convert(tokens[0], Integer.class), tokens[2]);
			} else {
				node = new ProductNode(convert(tokens[0], Integer.class), tokens[2], convert(tokens[1], Integer.class));
			}
			nodeCache.put(node.getId(), node);

		}

		// Setting references

		while ((line = br.readLine()) != null && line.trim().length() > 0) {
			String[] tokens = line.split(" ");
			if (tokens.length != 2)
				throw new IllegalArgumentException("Invalid Input Data");
			Integer parentId = convert(tokens[0], Integer.class);
			Integer childId = convert(tokens[0], Integer.class);
			if (!nodeCache.containsKey(parentId) || !nodeCache.containsKey(childId))
				throw new IllegalArgumentException("Invalid Input Data Id node doesn't exist");
			else {
				Node node = nodeCache.get(parentId);
				Node child = nodeCache.get(childId);
				child.setParent(node);
			}

		}
		root = nodeCache.get(1);
		// Check a cycle using either BFS and DFS if there then invalid data

	}

	@SuppressWarnings("unchecked")
	private <T extends Number> T convert(String str, Class<T> clazz) {

		try {
			if (clazz.equals(Integer.class))
				return (T) Integer.valueOf(str);
			else if (clazz.equals(Double.class))
				return (T) Double.valueOf(str);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid Input Data", e);
		}
		return null;

	}
	
	

}
