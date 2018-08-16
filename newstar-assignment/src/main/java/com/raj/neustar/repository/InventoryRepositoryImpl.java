package com.raj.neustar.repository;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.raj.neustar.algo.Strategy;
import com.raj.neustar.dto.ProductInfo;
import com.raj.neustar.util.FileUtil;
import com.raj.neustar.util.Utility;

/**
 * @author kumargau
 *
 */
public class InventoryRepositoryImpl implements InventoryRepository{

	private Map<Integer, Node> nodeCache = new ConcurrentHashMap<>();

	private Node root;

//	public Map<Integer, Node> getCache() {
//		return nodeCache;
//	}
	
	@Override
	public void load(String file) {

		FileUtil.readRaw(file, br -> {
			String line = br.readLine();
			Integer numOfNodes = Utility.convert(line, Integer.class);
			while (numOfNodes-- > 0 && (line = br.readLine()) != null) {
				String[] tokens = line.split(" ");
				if (tokens.length != 3)
					throw new IllegalArgumentException("Invalid Input Data");

				Node node = null;
				if (tokens[1].hashCode() == "-1".hashCode() && tokens[1].equals("-1")) {
					node = new CategoryNode(Utility.convert(tokens[0], Integer.class), tokens[2]);
				} else {
					node = new ProductNode(Utility.convert(tokens[0], Integer.class), tokens[2],
							Utility.convert(tokens[1], Double.class));
				}
				nodeCache.put(node.getId(), node);

			}

			// Setting references
			// System.out.println(nodeCache.toString());
			while ((line = br.readLine()) != null && line.trim().length() > 0) {
				String[] tokens = line.split(" ");
				if (tokens.length != 2)
					throw new IllegalArgumentException("Invalid Input Data");
				Integer parentId = Utility.convert(tokens[0], Integer.class);
				Integer childId = Utility.convert(tokens[1], Integer.class);
				// System.out.println(parentId + " " + childId);
				if (!nodeCache.containsKey(parentId) || !nodeCache.containsKey(childId))
					throw new IllegalArgumentException("Invalid Input Data Id node doesn't exist");
				else {
					Node node = nodeCache.get(parentId);
					Node child = nodeCache.get(childId);
					child.setParent(node);
				}

			}
			root = nodeCache.get(1);
			// Validate a cycle using if it is there then invalid data
			validateCycle(root);
			return true;
		});
	}

	/**
	 * @param root
	 *            BFS(Level order) traversal
	 * @return
	 */
	private void validateCycle(Node root) {

		Map<Integer, Boolean> visitedMap = new HashMap<>();
		Queue<Node> queue = new LinkedList<>();
		queue.offer(root);
		while (!queue.isEmpty()) {
			Node node = queue.poll();
			Boolean isVisited = false;
			if ((isVisited = visitedMap.get(node.getId())) != null && isVisited) {
				throw new IllegalArgumentException(
						"Invalid data : There is Cycle in category, sub-category and product");
			}
			visitedMap.put(node.getId(), true);
			if (!node.isLeafNode())
				((CategoryNode) node).getChildren().forEachRemaining(childNode -> queue.offer(childNode));
		}
	}

	private List<ProductNode> getProductsByCatgory(Integer categoryId) {
		Node node = nodeCache.get(categoryId);
		List<ProductNode> productNodes = new ArrayList<>();
		if (!node.isLeafNode())
			((CategoryNode) node).getAllLeafNodes().forEachRemaining(pNode -> productNodes.add((ProductNode) pNode));
		return productNodes;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean applyDiscount(Integer categoryOrProductId, Strategy strategy) {
		Node node = nodeCache.get(categoryOrProductId);
		node.setStrategy(strategy);
		return node.applyStrategy();
	}

	@Override
	public Integer addCategoryOrProduct(Integer parentId, Integer childId, String name, Double price) {
		if (nodeCache.containsKey(childId)) {
			throw new IllegalArgumentException("ProductId :" + childId + " is already exist. Please choose unused id");
		}
		Node cNode = nodeCache.get(parentId);
		if (cNode == null) {
			throw new IllegalArgumentException("CatgoryId :" + parentId + " is not valid");
		}
		Node node = null;
		if (price == -1)
			node = new CategoryNode(childId, name);
		else
			node = new ProductNode(childId, name, price);
		node.setParent(cNode);
		return node.getId();
	}

	@Override
	public boolean removeCategoryOrProduct(Integer categoryOrProductId) {
		Node node = nodeCache.get(categoryOrProductId);
		if (node == null) {
			throw new IllegalArgumentException("Node Id :" + categoryOrProductId + " doesn't exist");
		}
		CategoryNode parent = ((CategoryNode) node.getParent());
		if (parent != null)
			parent.removeChild(node);
		node.setParent(null);
		return true;
	}

	@Override
	public ProductInfo getMaxDiscountedItem(Integer categoryId) {
		Node node = nodeCache.get(categoryId);
		if (node == null) {
			throw new IllegalArgumentException("Node Id :" + categoryId + " doesn't exist");
		}
		
		if (node instanceof CategoryNode) {
			node = getProductsByCatgory(categoryId).stream().max((pN1, pN2) -> {			
				if (pN1.getPrice() - pN1.getSellingPrice() == (pN2.getPrice()- pN2.getSellingPrice()))
					return pN2.getId().compareTo(pN1.getId());
				else if (pN1.getPrice() - pN1.getSellingPrice() > (pN2.getPrice()- pN2.getSellingPrice() ))
					return 1;
				else
					return -1;
			}).get();
		}
		ProductNode pNode = (ProductNode)node;
		return new ProductInfo(pNode.getId(), pNode.getName(), pNode.getPrice(), pNode.getSellingPrice());
	}

	@SuppressWarnings("rawtypes")
	protected static abstract class Node {

		private Integer id;
		private String name;
		private Strategy strategy;

		public Node(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public abstract Node getParent();

		public abstract boolean applyStrategy();

		public abstract Boolean isLeafNode();

		public abstract void setParent(Node root);

		public void setStrategy(Strategy strategy) {
			this.strategy = strategy;
		}

		public Strategy getStrategy() {
			return this.strategy;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		@Override
		public int hashCode() {
			return id.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Node other = (Node) obj;
			if (id == null) {
				if (other.id != null)
					return false;
			} else if (!id.equals(other.id))
				return false;
			return true;
		}

	}

	protected static class ProductNode extends Node {
		private Double price;
		private Node parent;
		private Double sellingPrice;

		public ProductNode(Integer id, String name, Double price) {
			super(id, name);
			this.price = price;
			this.sellingPrice = price;
		}

		@Override
		public void setParent(Node parent) {
			if (parent instanceof CategoryNode) {
				((CategoryNode) parent).addChild(this);
			}
			this.parent = parent;
		}

		public Double getPrice() {
			return price;
		}

		@Override
		public Node getParent() {
			return parent;
		}

		@Override
		public Boolean isLeafNode() {
			return true;
		}
		
		@Override
		public boolean applyStrategy() {
			Double discount = calculateDiscount();
			if(getPrice()/2 <= discount) 
				return false;
			
			this.sellingPrice = getPrice() - discount;
			return true;
		}
		
		public boolean isValidDiscount() {
			Double discount = calculateDiscount();
			if(getPrice()/2 <= discount) 
				return false;
			return true;
		}

		private Double calculateDiscount() {
			Strategy<Double, Double> strategy;
			Double discount = 0.0;
			if ((strategy = this.getStrategy()) != null) {
				discount = getPrice() - (Double) strategy.execute(getSellingPrice());
			}
			return discount;
		}

		public Double getSellingPrice() {
			return sellingPrice;
		}

	}

	protected static class CategoryNode extends Node {
		private Set<Node> children;
		private Node parent;

		public CategoryNode(Integer id, String name) {
			super(id, name);
			this.children = new HashSet<>();

		}

		public Iterator<Node> getChildren() {
			return this.children.iterator();
		}

		public Iterator<Node> getAllLeafNodes() {
			List<Node> leafNodes = new ArrayList<>();
			Queue<Node> queue = new LinkedList<>();
			queue.offer(this);
			while (!queue.isEmpty()) {
				Node node = queue.poll();
				if (node.isLeafNode())
					leafNodes.add(node);
				else
					((CategoryNode) node).getChildren().forEachRemaining(childNode -> queue.offer(childNode));
			}

			return leafNodes.iterator();
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

		@Override
		public boolean applyStrategy() {

			class StatusHolder {
				Boolean status = true;
			}
			final StatusHolder statusHolder = new StatusHolder();

			getAllLeafNodes().forEachRemaining(node -> {
				if (this.getStrategy()!= null)
					node.setStrategy(this.getStrategy());
				statusHolder.status = statusHolder.status && ((ProductNode) node).isValidDiscount();
			});
			if (statusHolder.status)
				getAllLeafNodes().forEachRemaining(node -> statusHolder.status = statusHolder.status && node.applyStrategy());
			return statusHolder.status;

		}

		@Override
		public Node getParent() {
			return parent;
		}

		@Override
		public Boolean isLeafNode() {
			return false;
		}
	}

}
