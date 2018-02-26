package taskDependency;

import java.util.HashSet;

public class TaskNode 
{
	private char taskName;
	
	private HashSet<Character> preRequisites;
	
	private HashSet<Character> dependents;
	
	public TaskNode(char taskName)
	{
		this.taskName = taskName;
		this.preRequisites = new HashSet<Character>();
		this.dependents = new HashSet<Character>();
	}
	
	public void addDependent(char task)
	{
		this.dependents.add(task);
	}
	
	public void addPrerequisite(char task)
	{
		this.preRequisites.add(task);
	}
	
	
	
	public char getTaskName() {
		return taskName;
	}

	public HashSet<Character> getPreRequisites() {
		return preRequisites;
	}

	public HashSet<Character> getDependents() {
		return dependents;
	}

	@Override
	public boolean equals(Object node)
	{
		if((node == null) || !(node instanceof TaskNode))
			return false;
		else
		{
			TaskNode otherTask = (TaskNode)node;
			return (this.taskName == otherTask.taskName);
		}
	}
	
	@Override
	public int hashCode()
	{
		return (int)(this.taskName);
	}
}
