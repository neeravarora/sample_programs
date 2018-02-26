package com.raj.modelSet2;

public class SharedAsset extends ID<Long>{
	
	private String assetName;
	private String type;
	private String worth;
	public String getAssetName() {
		return assetName;
	}
	public void setAssetName(String assetName) {
		this.assetName = assetName;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getWorth() {
		return worth;
	}
	public void setWorth(String worth) {
		this.worth = worth;
	}

}
