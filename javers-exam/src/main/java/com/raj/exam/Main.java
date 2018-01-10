package com.raj.exam;

import org.javers.core.Javers;
import org.javers.core.JaversBuilder;
import org.javers.core.diff.Diff;
import org.javers.core.diff.ListCompareAlgorithm;
import org.javers.core.diff.changetype.ValueChange;

public class Main {
	
	public static void main(String[] args) {
		//Javers javers = JaversBuilder.javers().withListCompareAlgorithm(ListCompareAlgorithm.LEVENSHTEIN_DISTANCE).build();
		Javers javers = JaversBuilder.javers().build();
		
		 Employee oldBoss = new Employee("Big Boss", 99)
			        .addSubordinates(
			            new Employee("Noisy Manager", 9999),
			            new Employee("Great Developer", 8888));

			    Employee newBoss = new Employee("Big Boss", 99)
			        .addSubordinates(
			            new Employee("Noisy Manager1", 2000),
			            new Employee("Great Developer2", 10000),
			            new Employee("New Developer", 8000));

			    //when
			    Diff diff = javers.compare(oldBoss, newBoss);
			    System.out.println(diff);
			    //then
			   // ValueChange change =  diff.getChangesByType(ValueChange.class).get(0);
			  //  System.out.println(change);
			    
			    String json = javers.getJsonConverter().toJson(diff);
			    System.out.println(json);
	}

}
