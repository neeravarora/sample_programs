package com.raj.modelSet2;

public class Team extends ID<Long>
{

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


}
