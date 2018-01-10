package com.raj;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;




public class Employee extends Person{
	
	private Long salary;
	
    private Employee boss;

    //private final List<Employee> subordinates = new ArrayList<>();
    
    private Project currentProject;
    
    private Team currentTeam;
    
    private Map<Team, Project> workHistory = new HashMap<>();
    
    private Map<Integer, Team> teamMap = new HashMap<>();
    
    private List<Asset> assets = new ArrayList<>();

	public Long getSalary() {
		return salary;
	}

	public void setSalary(Long salary) {
		this.salary = salary;
	}
	
	public Employee(Long id) {
         this.setId(id);
     }
	
//    public Employee(String name, Long salary) {
//        // checkNotNull(name);
//         this.setName(name);
//         this.salary = salary;
//     }

//     public Employee addSubordinate(Employee employee) {
//        // checkNotNull(employee);
//         employee.boss = this;
//         subordinates.add(employee);
//         return this;
//     }
//     
//     public Employee addSubordinates(Employee... employees) {
//         // checkNotNull(employees);
//   
//     	for (Employee employee : employees) {
//     		employee.boss = this;
//             subordinates.add(employee);
// 		}
//          
//          return this;
//      }

	
	

	public Project getCurrentProject() {
		return currentProject;
	}

	public void setCurrentProject(Project currentProject) {
		this.currentProject = currentProject;
	}
	
	

	public Team getCurrentTeam() {
		return currentTeam;
	}

	public void setCurrentTeam(Team currentTeam) {
		this.currentTeam = currentTeam;
	}

	public Map<Team, Project> getWorkHistory() {
		return workHistory;
	}

	public void addWorkHistory(Team team, Project project) {
		this.workHistory.put(team, project);
	}
	
	public void removeWorkHistory(Team team) {
		this.workHistory.remove(team);
	}
	
	 public Employee addAsset(Asset asset) {
		 assets.add(asset);
	         return this;
	     }
	 
	 public Employee removeAsset(Asset asset) {
		 assets.remove(asset);
	         return this;
	     }
	 
	 public void addTeam(Integer index, Team team) {
			this.teamMap.put(index, team);
		}
		
		public void removeTeam(Integer index) {
			this.teamMap.remove(index);
		}
     

}
