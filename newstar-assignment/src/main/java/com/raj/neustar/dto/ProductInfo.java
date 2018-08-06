package com.raj.neustar.dto;

/**
 * @author kumargau
 *
 */
public class ProductInfo {
	private Integer id;
	private String name;
	private Double price;
	private Double sellingPrice;

	public ProductInfo(Integer id, String name, Double price, Double sellingPrice) {
		super();
		this.id = id;
		this.name = name;
		this.price = price;
		this.sellingPrice = sellingPrice;
	}

	public Integer getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public Double getPrice() {
		return price;
	}

	public Double getSellingPrice() {
		return sellingPrice;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.getId());
		sb.append(" ");
		sb.append(this.getName());
		sb.append(" ");
		sb.append(this.getPrice());
		sb.append(" ");
		sb.append(this.getSellingPrice());
		return sb.toString();
	}
	
}
