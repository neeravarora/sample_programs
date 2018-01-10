package com.raj.modelSet2;

import org.javers.core.metamodel.annotation.Id;

public class Person extends ID<Long>
{
	
	
	
	private String name;
	private Long age;
	private ContactInfo contactInfo;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Long getAge() {
		return age;
	}
	public void setAge(Long age) {
		this.age = age;
	}
	public ContactInfo getContactInfo() {
		return contactInfo;
	}
	public void setContactInfo(ContactInfo contactInfo) {
		this.contactInfo = contactInfo;
	}
	
	

	

}
