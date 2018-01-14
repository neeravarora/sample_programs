package com.raj.diff.model;

import java.util.ArrayList;
import java.util.List;

import org.javers.core.metamodel.annotation.Id;

public class Exam {
	
	@Id
	public Integer id;
	public List<List<Child>> lists = new ArrayList<>(); 

}
