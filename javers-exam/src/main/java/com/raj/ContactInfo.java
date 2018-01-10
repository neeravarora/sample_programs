package com.raj;

public class ContactInfo extends ID<Long>{

	private Address address;
	private String mobileNo;
	public ContactInfo(Long id) {
		this.setId(id);
	}
	public Address getAddress() {
		return address;
	}
	public void setAddress(Address address) {
		this.address = address;
	}
	public String getMobileNo() {
		return mobileNo;
	}
	public void setMobileNo(String mobileNo) {
		this.mobileNo = mobileNo;
	}
	
	
}
