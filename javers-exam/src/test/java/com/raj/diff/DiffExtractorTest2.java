package com.raj.diff;

import static org.junit.Assert.fail;

import java.util.Map;

import org.javers.core.Javers;
import org.javers.core.JaversBuilder;
import org.javers.core.diff.Diff;
import org.junit.Before;
import org.junit.Test;

import com.raj.modelSet2.Address;
import com.raj.modelSet2.Asset;
import com.raj.modelSet2.ContactInfo;
import com.raj.modelSet2.Employee;
import com.raj.modelSet2.Project;
import com.raj.modelSet2.Team;
import com.raj.diff.custom.FMCJaversBuilder;
import com.raj.diff.custom.FMCJaversCore;

public class DiffExtractorTest2 {
	
	Employee eOld = null;
	Employee eModified = null;

	
	@Before
	public void setup() {
		createSeedData();
	}
	
	@Test
	public void test() {
		
		Javers javers = //JaversBuilder.javers().build();
				FMCJaversBuilder.javers().build();
		Diff diff = javers.compare(eOld, eModified);
		((FMCJaversCore)javers).compareAndConvert(eOld, eModified);
		
		
		System.out.println("++++++++++++++++++++++++++++");
		System.out.println("++++++++++++++++++++++++++++");
		 System.out.println(diff);
		 
		 
		// String json = javers.getJsonConverter().toJson(diff);
		  //  System.out.println(json);
	}

	private void createSeedData() {
		
		Asset asset1 = new Asset();
		asset1.setAssetName("Mac");
		asset1.setType("Lapy");
		asset1.setWorth("1.65 lac");
		
		Asset asset2 = new Asset();
		asset2.setAssetName("Lenovo");
		asset2.setType("Lapy");
		asset2.setWorth("1.3 lac");
		
		Asset asset3 = new Asset();
		asset3.setAssetName("Dell");
		asset3.setType("Lapy");
		asset3.setWorth("0.6 lac");
		
		Asset asset4 = new Asset();
		asset4.setAssetName("Iphone");
		asset4.setType("moblile");
		asset4.setWorth("0.6 lac");
		
		Asset asset5 = new Asset();
		asset5.setAssetName("Android");
		asset5.setType("moblile");
		asset5.setWorth("0.2 lac");
		
		Asset asset6 = new Asset();
		asset6.setAssetName("Window");
		asset6.setType("moblile");
		asset6.setWorth("0.3 lac");
		
		Team team1 = new Team(1l);
		team1.setTeamName("VPN Team");
		Team team2 = new Team(2l);
		team2.setTeamName("Multi context team");
		Team team3 = new Team(3l);
		team3.setTeamName("Router team");
		Team team4 = new Team(4l);
		team4.setTeamName("Bulwark");
		Team team5 = new Team(5l);
		team5.setTeamName("Sales");
		Team team6 = new Team(6l);
		team6.setTeamName("Marketing");
		
		
		Project project1 = new Project(1l);
		project1.setProjectName("Site 2 Site VPN");
		Project project2 = new Project(2l);
		project2.setProjectName("Multicontext");
		Project project3 = new Project(3l);
		project3.setProjectName("Routing devices Management Sys");
		Project project4 = new Project(4l);
		project4.setProjectName("MultiPeer");
		Project project5 = new Project(5l);
		project5.setProjectName("RAVPN");
		Project project6 = new Project(6l);
		project6.setProjectName("Policy Comparison");
		Project project7 = new Project(7l);
		project7.setProjectName("FMC");
		
		// address obj
		Address address1 = new Address(1l);
		address1.setAddress("old dummy address");
		
		// contact obj
		ContactInfo contactInfo1 = new ContactInfo(1l);
		contactInfo1.setMobileNo("1111111111");
		
		contactInfo1.setAddress(address1);
		
		// address obj
		Address address2 = new Address(2l);
        address2.setAddress("old dummy address -2");
        
				
		// contact obj
		ContactInfo contactInfo2 = new ContactInfo(2l);
		contactInfo2.setMobileNo("2222222222");
		
		contactInfo2.setAddress(address2 );
		
		// address obj
		Address address3 = new Address(3l);
        address3.setAddress("old dummy address -3");
				
		// contact obj
		ContactInfo contactInfo3 = new ContactInfo(3l);
		contactInfo3.setMobileNo("3333333333");
		
		contactInfo3.setAddress(address3 );
		
		// address obj
		Address address4 = new Address(4l);
        address4.setAddress("old dummy address -4");
				
		// contact obj
		ContactInfo contactInfo4 = new ContactInfo(4l);
		contactInfo4.setMobileNo("4444444444");
		
		contactInfo4.setAddress(address4 );
		
		// address obj
		Address address5 = new Address(5l);
        address5.setAddress("old dummy address -5");
				
		// contact obj
		ContactInfo contactInfo5 = new ContactInfo(5l);
		contactInfo5.setMobileNo("5555555555");
		
		contactInfo5.setAddress(address5 );
		
		
		
		
		
		
		eOld = new Employee(1l);
		eOld.setName("Boss");
		eOld.setSalary(99l);
		eOld.setAge(20l);
		
		eOld.setContactInfo(contactInfo1);
		eOld.setCurrentTeam(team1);
		eOld.setCurrentProject(project1);
		
		eOld.addWorkHistory(team2, project2);
		eOld.addWorkHistory(team3, project3);
		eOld.addWorkHistory(team4, project4);
		
		eOld.addAsset(asset1);
		eOld.addAsset(asset2);
		eOld.addAsset(asset4);
		
		eOld.addTeam(1, team1);
		eOld.addTeam(2, team2);
		eOld.addTeam(3, team4);
		
		
		
		Employee reportee1 = new Employee(2l);
		reportee1.setName("Developer1");
		reportee1.setSalary(79l);
		reportee1.setAge(18l);
		
		reportee1.setContactInfo(contactInfo2 );
		reportee1.setCurrentTeam(team1 );
		reportee1.setCurrentProject(project1);
		reportee1.addWorkHistory(team4, project4);
		
		
		Employee reportee2 = new Employee(3l);
		reportee2.setName("Tester1");
		reportee2.setSalary(49l);
		reportee2.setAge(16l);
		
		reportee2.setContactInfo(contactInfo3 );
		reportee2.setCurrentTeam(team1 );
		reportee2.setCurrentProject(project1);
		reportee2.addWorkHistory(team5, project5);
		reportee2.addWorkHistory(team6, project6);
		
//		eOld.addSubordinate(reportee1);
//		eOld.addSubordinate(reportee2);
		
		
		
		
		///-----------------------------
		///-----------------------------
		
		eModified = new Employee(1l);
		
		
		eModified = new Employee(1l);
		eModified.setName("Boss");
		eModified.setSalary(101l);
		eModified.setAge(24l);
		
		eModified.setContactInfo(contactInfo4);
		eModified.setCurrentTeam(team4);
		eModified.setCurrentProject(project7);
		
		eModified.addWorkHistory(team2, project2);
		eModified.addWorkHistory(team3, project4);
		eModified.addWorkHistory(team5, project7);
		
		eModified.addAsset(asset3);
		eModified.addAsset(asset1);
		eModified.addAsset(asset4);
		
		eModified.addTeam(1, team3);
		eModified.addTeam(2, team1);
		eModified.addTeam(3, team4);
		
		
		
//		Employee reportee11 = new Employee(4l);
//		reportee11.setName("Developer1");
//		reportee11.setSalary(79l);
//		reportee11.setAge(18l);
//		
//		reportee1.setContactInfo(contactInfo2 );
//		reportee1.setCurrentTeam(team1 );
//		reportee1.setCurrentProject(project1);
//		reportee1.addWorkHistory(team4, project4);
		
		
		Employee reportee12 = new Employee(2l);
		reportee12.setName("Tester1");
		reportee12.setSalary(39l);
		reportee12.setAge(15l);
		
		reportee12.setContactInfo(contactInfo3 );
		reportee12.setCurrentTeam(team2 );
		reportee12.setCurrentProject(project2);
		reportee12.addWorkHistory(team4, project4);
		reportee12.addWorkHistory(team6, project6);
		
		Employee reportee13 = new Employee(4l);
		reportee12.setName("New Developer");
		reportee12.setSalary(91l);
		reportee12.setAge(14l);
		
		reportee12.setContactInfo(contactInfo5 );
		reportee12.setCurrentTeam(team1 );
		reportee12.setCurrentProject(project1);
		reportee12.addWorkHistory(team5, project5);
		reportee12.addWorkHistory(team6, project6);
		
		Employee reportee14 = new Employee(3l);
		reportee14.setName("Architect");
		reportee14.setSalary(120l);
		reportee14.setAge(30l);
		
		reportee14.setContactInfo(contactInfo2 );
		reportee14.setCurrentTeam(team1 );
		reportee14.setCurrentProject(project1);
		reportee14.addWorkHistory(team2, project2);
		reportee14.addWorkHistory(team3, project3);
		
		//eModified.addSubordinate(reportee11);
		
//		eModified.addSubordinate(reportee12);
//		eModified.addSubordinate(reportee13);
//		eModified.addSubordinate(reportee14);
	}
	
	
	
	

}
