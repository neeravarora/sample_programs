package com.raj;

import org.javers.core.metamodel.annotation.Id;

public class Team //extends ID<Long>
{
	@Id
	private Long id;
	private String teamName;

	public Team(Long id) {
		setId(id);
	}

	public String getTeamName() {
		return teamName;
	}

	public void setTeamName(String teamName) {
		this.teamName = teamName;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}
	
	
	

}
