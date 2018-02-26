package com.raj.diff;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.javers.core.Javers;
import org.javers.core.JaversBuilder;
import org.javers.core.diff.Diff;

import com.raj.diff.custom.FMCJaversBuilder;
import com.raj.diff.custom.FMCJaversCore;
import com.raj.diff.model.Child;
import com.raj.diff.model.Exam;

public class Test2 {
	@org.junit.Test
	public void test() {
		Javers javers = JaversBuilder.javers().build();
				FMCJaversBuilder.javers().build();
		//Diff diff = javers.compare(eOld, eModified);
		Exam exam = new Exam();
		List<List<Child>> lists = new ArrayList<>();
		List<Child> list1 = new ArrayList<>();
		Child c101 = new Child(101,"c101");
		list1.add(c101);
		List<Child> list2 = new ArrayList<>();
		Child c102 = new Child(102,"c102");
		list2.add(c102);
		List<Child> list3 = new ArrayList<>();
		Child c103 = new Child(103,"c103");
		list3.add(c103);
		
		lists.add(list1);
		
		lists.add(list2);
		lists.add(list3);
		exam.id = 1;
		exam.lists = lists;
		
		Exam exam2 = new Exam();
		List<List<Child>> lists2 = new ArrayList<>();
		List<Child> list11 = new ArrayList<>();
		Child c201 = new Child(1011,"c201");
		list11.add(c201);
		List<Child> list12 = new ArrayList<>();
		Child c202 = new Child(2011,"c202");
		Child c203 = new Child(1021,"c203");
		list12.add(c202);
		list12.add(c203);
		List<Child> list13 = new ArrayList<>();
		Child c204 = new Child(2041,"c204");
		list13.add(c204);
		lists2.add(list11);
		lists2.add(list12);
		lists2.add(list13);
		
		exam2.id = 1;
		exam2.lists = lists2;
		
		
		Diff diff =((FMCJaversCore)javers).compare(exam, exam2);
		System.out.println(diff);
	}
}
