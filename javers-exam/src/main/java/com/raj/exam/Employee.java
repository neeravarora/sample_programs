package com.raj.exam;

import java.util.ArrayList;
import java.util.List;

import org.javers.core.metamodel.annotation.Id;

public class Employee {

    @Id
    private final String name;

    private final int salary;

    private Employee boss;

    private final List<Employee> subordinates = new ArrayList<>();

    public Employee(String name) {
        this(name, 10000);
    }

    public Employee(String name, int salary) {
       // checkNotNull(name);
        this.name = name;
        this.salary = salary;
    }

    public Employee addSubordinate(Employee employee) {
       // checkNotNull(employee);
        employee.boss = this;
        subordinates.add(employee);
        return this;
    }
    
    
    public Employee addSubordinates(Employee... employees) {
        // checkNotNull(employees);
  
    	for (Employee employee : employees) {
    		employee.boss = this;
            subordinates.add(employee);
		}
         
         return this;
     }

    // ...
}