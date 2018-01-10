package com.raj.modelSet2;

public class Address extends ID<Long> {

	private String address;

	public Address(Long id) {
		this.setId(id);
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}
	
	
}
