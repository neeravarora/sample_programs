package com.raj.diff.custom;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.javers.core.diff.Change;
import org.javers.core.diff.Diff;
import org.javers.core.diff.changetype.NewObject;
import org.javers.core.diff.changetype.ObjectRemoved;
import org.javers.core.diff.changetype.PropertyChange;
import org.javers.core.graph.ObjectNode;
import org.javers.core.metamodel.object.GlobalId;
import org.javers.core.metamodel.type.JaversProperty;

import com.raj.diff.custom.graph.FMCEdge;
import com.raj.diff.custom.graph.FMCMultiEdge;
import com.raj.diff.custom.graph.FMCObjectNode;
import com.raj.diff.custom.graph.FMCShallowSingleEdge;
import com.raj.diff.custom.graph.FMCSingleEdge;
import com.raj.diff.model.ChangeEntry;

public class FMCDiffConvertor_Bak1 {

	void transform(FMCObjectNode leftRoot, FMCObjectNode rightRoot, Diff diff) {
		ChangeEntry rootChangeEntry = new ChangeEntry();
		Map<GlobalId, Map<Class, Map<String, Change>>> diffMap = new HashMap<>();
		Map<GlobalId, Change> newObjectChangeMap = new HashMap<>();
		Map<GlobalId, Change> objectRemovedChangeMap = new HashMap<>();
		for (Change change : diff.getChanges()) {
			if (change instanceof PropertyChange) {
				Map<Class, Map<String, Change>> changeTypeMap = null;
				Map<String, Change> propertyMap = null;
				if (diffMap.get(change.getAffectedGlobalId()) == null) {
					changeTypeMap = new HashMap<>();
					propertyMap = new HashMap<>();
				} else {
					changeTypeMap = diffMap.get(change.getAffectedGlobalId());
					if (changeTypeMap.get(change.getClass()) == null) {
						propertyMap = new HashMap<>();
					} else {
						propertyMap = changeTypeMap.get(change.getClass());
					}
				}
				propertyMap.put(((PropertyChange) change).getPropertyName(), change);
				changeTypeMap.put(change.getClass(), propertyMap);
				diffMap.put(change.getAffectedGlobalId(), changeTypeMap);
			}else if(change instanceof NewObject){
				newObjectChangeMap.put(change.getAffectedGlobalId(), change);
			}else if(change instanceof ObjectRemoved){
				objectRemovedChangeMap.put(change.getAffectedGlobalId(), change);
			}
			
			
		}
		
		System.out.println("new obj-------------------------------");
		System.out.println(newObjectChangeMap);
		System.out.println("removed obj-------------------------------");
		System.out.println(objectRemovedChangeMap);
		System.out.println("diff obj-------------------------------");
		System.out.println(diffMap);
		
		
		Queue<ObjectNode> queue = new ConcurrentLinkedQueue<>();
		if(leftRoot !=null)
		     queue.offer(leftRoot);
		Map<GlobalId, Boolean> nodeVisitedMap = new HashMap<>();
		nodeVisitedMap.put(leftRoot.getGlobalId(), true);
		
		while (!queue.isEmpty()) {
			ObjectNode node= queue.poll();
			System.out.println(node.getGlobalId());
			Map<JaversProperty, FMCEdge> edges = ((FMCObjectNode)node).getEdges();
			if(edges != null && !edges.isEmpty())
			for (Entry<JaversProperty, FMCEdge> entry : edges.entrySet()) {
				//System.out.println(entry.getKey()+" : "+entry.getValue());
				FMCEdge edge = entry.getValue();
				ObjectNode childNode = null;
				if(edge instanceof FMCShallowSingleEdge){
					System.out.println("shallow single edge " + edge);
					
					//queue.offer(((FMCShallowSingleEdge)edge).ge )
				}else if(edge instanceof FMCSingleEdge){
					//System.out.println("single edge " + edge);
					childNode = ((FMCSingleEdge) edge).getReferenceNode();
					if(nodeVisitedMap.get(childNode) == null || nodeVisitedMap.get(childNode))
					queue.offer(childNode);
				}else if(edge instanceof FMCMultiEdge){
					//System.out.println("multi edge " + edge);
					List<ObjectNode> references =((FMCMultiEdge)edge).getReferences();
					if(references != null && !references.isEmpty())
					for (ObjectNode objectNode : ((FMCMultiEdge)edge).getReferences()) {
						queue.offer(objectNode);
					}
				}
			}
			
		}
		
		
		
		
//		while (leftRoot.getEdges() != null && leftRoot.getEdges().isEmpty() ) {
//
//			Map<JaversProperty, FMCEdge> leftNodeEdges = leftRoot.getEdges();
//			Map<JaversProperty, FMCEdge> rightNodeEdges = rightRoot.getEdges();
//			
//
//			if (true) {
//				break;
//			}
//		}

	}
}
