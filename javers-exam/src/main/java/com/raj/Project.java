package com.raj;

public class Project extends ID<Long>{
	
	private String projectName;

	public Project(Long id) {
		setId(id);
	}

	public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}
	
	

}
