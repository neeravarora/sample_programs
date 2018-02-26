package test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TaskDependency {

	HashSet<Task> noPreReqs = new HashSet<Task>();

	public void addTask(Task prerequisite, Task dependent) {
		prerequisite.dependency.add(dependent);
		dependent.prerequisite.add(prerequisite);
		noPreReqs.remove(dependent);
		if (prerequisite.prerequisite.isEmpty()) {
			noPreReqs.add(prerequisite);
		}
	}

	public List<Task> getExecutionSequence() {
		List<Task> executionSequence = new ArrayList<Task>();
		List<Task> toBeProcessed = new ArrayList<Task>(noPreReqs);

		while (!toBeProcessed.isEmpty()) {
			Task task = toBeProcessed.remove(0);
			executionSequence.add(task);
			for (Task dependent : task.dependency) {
				dependent.prerequisite.remove(task);
				if (dependent.prerequisite.isEmpty()) {
					toBeProcessed.add(dependent);
				}
			}
		}

		return executionSequence;
	}

	public static void main(String args[]) {
		TaskDependency td = new TaskDependency();
		List<Task> inputTasks = new ArrayList<Task>();
		for (int i = 0; i < 10; i++) {
			inputTasks.add(new Task("T" + i));
		}

		td.addTask(inputTasks.get(0), inputTasks.get(1));
		td.addTask(inputTasks.get(0), inputTasks.get(2));
		td.addTask(inputTasks.get(1), inputTasks.get(3));
		td.addTask(inputTasks.get(2), inputTasks.get(3));
		td.addTask(inputTasks.get(3), inputTasks.get(4));
		td.addTask(inputTasks.get(2), inputTasks.get(4));
		td.addTask(inputTasks.get(1), inputTasks.get(5));
		td.addTask(inputTasks.get(3), inputTasks.get(5));
		td.addTask(inputTasks.get(0), inputTasks.get(6));
		td.addTask(inputTasks.get(4), inputTasks.get(6));
		td.addTask(inputTasks.get(6), inputTasks.get(7));
		td.addTask(inputTasks.get(8), inputTasks.get(7));
		td.addTask(inputTasks.get(8), inputTasks.get(9));

		System.out.println(td.getExecutionSequence());
	}
}

class Task {
	String name;
	Set<Task> prerequisite;
	Set<Task> dependency;

	public Task(String name) {
		this.name = name;
		prerequisite = new HashSet<Task>();
		dependency = new HashSet<Task>();
	}

	public int hashCode() {
		return name.hashCode();
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Task other = (Task) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	public String toString() {
		return name;
	}
}