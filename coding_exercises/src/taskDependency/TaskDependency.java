package taskDependency;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class TaskDependency 
{
	private HashMap<Character, TaskNode> graph;

	public TaskDependency()
	{
		this.graph = new HashMap<Character, TaskNode>();
	}

	public void addTask(char preRequisite, char dependent)
	{
		// add dependent to source
		TaskNode node = this.graph.getOrDefault(preRequisite, new TaskNode(preRequisite));
		node.addDependent(dependent);
		this.graph.put(preRequisite, node);

		// add pre-req to dependent
		TaskNode node2 = this.graph.getOrDefault(dependent, new TaskNode(dependent));
		node2.addPrerequisite(preRequisite);
		this.graph.put(dependent, node2);
	}

	public String getRunlist() throws UnsupportedOperationException
	{
		List<TaskNode> initialNodes = new ArrayList<TaskNode>();

		// get the nodes which have zero pre-requisites
		for(char key : this.graph.keySet())
		{
			if(this.graph.get(key).getPreRequisites().size() == 0)
			{
				initialNodes.add(this.graph.get(key));
			}
		}

		if(initialNodes.size() == 0)
		{
			throw new UnsupportedOperationException("Impossible sequence !");
		}

		HashSet<Character> doneTasks = new HashSet<Character>();
		HashSet<Character> queuedTasks = new HashSet<Character>();

		Queue<TaskNode> bfsQueue = new LinkedList<TaskNode>();
		StringBuilder runList = new StringBuilder();

		for(TaskNode node : initialNodes)
		{
			doneTasks.add(node.getTaskName());
			runList.append(node.getTaskName());

			for(char dependent : node.getDependents())
			{
				if(!doneTasks.contains(dependent))
				{
					if(!queuedTasks.contains(dependent))
					{
						queuedTasks.add(dependent);
						bfsQueue.add(this.graph.get(dependent));
					}
				}
			}
		}

		while(!bfsQueue.isEmpty())
		{
			TaskNode node = bfsQueue.remove();

			if(!doneTasks.contains(node.getTaskName()))
			{
				boolean areAllPreqDone = true;
				for(char preReq : node.getPreRequisites())
				{
					areAllPreqDone = areAllPreqDone & doneTasks.contains(preReq);
				}

				if(areAllPreqDone)
				{
					runList.append(node.getTaskName());
					doneTasks.add(node.getTaskName());
				}
			}

			for(char dependent : node.getDependents())
			{
				if(!doneTasks.contains(dependent))
				{
					if(!queuedTasks.contains(dependent))
					{
						queuedTasks.add(dependent);
						bfsQueue.add(this.graph.get(dependent));
					}
				}
			}
		}

		return runList.toString();
	}
}