package com.raj.diff.custom;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.javers.core.Javers;
import org.javers.core.diff.Change;
import org.javers.core.diff.Diff;
import org.javers.core.diff.changetype.NewObject;
import org.javers.core.diff.changetype.ObjectRemoved;
import org.javers.core.diff.changetype.PropertyChange;
import org.javers.core.diff.changetype.ReferenceChange;
import org.javers.core.diff.changetype.ValueChange;
import org.javers.core.graph.ObjectNode;
import org.javers.core.metamodel.object.Cdo;
import org.javers.core.metamodel.object.GlobalId;
import org.javers.core.metamodel.object.InstanceId;
import org.javers.core.metamodel.type.JaversProperty;

import com.google.common.collect.Sets;
import com.raj.diff.custom.graph.FMCEdge;
import com.raj.diff.custom.graph.FMCMultiEdge;
import com.raj.diff.custom.graph.FMCObjectNode;
import com.raj.diff.custom.graph.FMCShallowSingleEdge;
import com.raj.diff.custom.graph.FMCSingleEdge;
import com.raj.diff.model.ChangeEntry;
import com.raj.diff.model.ObjectChangeEntry;
import com.raj.diff.model.PropertyChangeEntry;
import com.raj.diff.model.Value;
import com.raj.diff.model.ValueChangeEntry;

public class FMCDiffConvertor {

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
		
		rootChangeEntry = getDiff("Root", leftRoot, rightRoot, diffMap, newObjectChangeMap, objectRemovedChangeMap, null);
		
		Queue<FMCObjectNode> queue = new ConcurrentLinkedQueue<>();
		if(leftRoot !=null)
		     queue.offer(leftRoot);
		//Map<FMCObjectNode, Boolean> nodeVisitedMap = new HashMap<>();
		//nodeVisitedMap.put(leftRoot, true);
		while (!queue.isEmpty()) {
			ObjectNode node= queue.poll();
			System.out.println(node.getGlobalId() +"  -  ");
			Map<JaversProperty, FMCEdge> edges = ((FMCObjectNode)node).getEdges();
			if(edges != null && !edges.isEmpty())
			for (Entry<JaversProperty, FMCEdge> entry : edges.entrySet()) {
				System.out.println("-------------" +entry.getKey().getName()+" : "+entry.getKey().getType()+" : "+entry.getValue());
				FMCEdge edge = entry.getValue();
				if(edge instanceof FMCShallowSingleEdge){
					System.out.println("shallow single edge " + edge);
					
					//queue.offer(((FMCShallowSingleEdge)edge).ge )
				}else if(edge instanceof FMCSingleEdge){
					//System.out.println("single edge " + edge);
					ObjectNode childNode = ((FMCSingleEdge) edge).getReferenceNode();
					//if(nodeVisitedMap.get(childNode) == null || !nodeVisitedMap.get(childNode)){
					   queue.offer((FMCObjectNode) childNode);
					  // nodeVisitedMap.put((FMCObjectNode)childNode, true);
					  // }
				}else if(edge instanceof FMCMultiEdge){
					//System.out.println("multi edge " + edge);
					List<ObjectNode> references =((FMCMultiEdge)edge).getReferences();
					if(references != null && !references.isEmpty())
					for (ObjectNode childNode : ((FMCMultiEdge)edge).getReferences()) {
						//if(nodeVisitedMap.get(childNode) == null || !nodeVisitedMap.get(childNode)){
						   queue.offer((FMCObjectNode) childNode);
						 //  nodeVisitedMap.put((FMCObjectNode)childNode, true);
						  // }
					}
				}
			}
			
		}
		
		
//		System.out.println("=====================================");
//		System.out.println("=====================================");
//		System.out.println("=====================================");
		

		
		
		
		
//		Queue<FMCObjectNode> queue = new ConcurrentLinkedQueue<>();
//		if(leftRoot !=null)
//		     queue.offer(leftRoot);
//		//Map<FMCObjectNode, Boolean> nodeVisitedMap = new HashMap<>();
//		//nodeVisitedMap.put(leftRoot, true);
//		while (!queue.isEmpty()) {
//			ObjectNode node= queue.poll();
//			System.out.println(node.getGlobalId() +"  -  ");
//			Map<JaversProperty, FMCEdge> edges = ((FMCObjectNode)node).getEdges();
//			if(edges != null && !edges.isEmpty())
//			for (Entry<JaversProperty, FMCEdge> entry : edges.entrySet()) {
//				System.out.println("-------------" +entry.getKey().getName()+" : "+entry.getKey().getType()+" : "+entry.getValue());
//				FMCEdge edge = entry.getValue();
//				if(edge instanceof FMCShallowSingleEdge){
//					System.out.println("shallow single edge " + edge);
//					
//					//queue.offer(((FMCShallowSingleEdge)edge).ge )
//				}else if(edge instanceof FMCSingleEdge){
//					//System.out.println("single edge " + edge);
//					ObjectNode childNode = ((FMCSingleEdge) edge).getReferenceNode();
//					//if(nodeVisitedMap.get(childNode) == null || !nodeVisitedMap.get(childNode)){
//					   queue.offer((FMCObjectNode) childNode);
//					  // nodeVisitedMap.put((FMCObjectNode)childNode, true);
//					  // }
//				}else if(edge instanceof FMCMultiEdge){
//					//System.out.println("multi edge " + edge);
//					List<ObjectNode> references =((FMCMultiEdge)edge).getReferences();
//					if(references != null && !references.isEmpty())
//					for (ObjectNode childNode : ((FMCMultiEdge)edge).getReferences()) {
//						//if(nodeVisitedMap.get(childNode) == null || !nodeVisitedMap.get(childNode)){
//						   queue.offer((FMCObjectNode) childNode);
//						 //  nodeVisitedMap.put((FMCObjectNode)childNode, true);
//						  // }
//					}
//				}
//			}
//			
//		}
//		
//		
//		System.out.println("=====================================");
//		System.out.println("=====================================");
//		System.out.println("=====================================");
//		
//		Queue<FMCObjectNode> rightQueue = new ConcurrentLinkedQueue<>();
//		if(rightRoot !=null)
//			rightQueue.offer(rightRoot);
//		//Map<FMCObjectNode, Boolean> nodeVisitedMap = new HashMap<>();
//		//nodeVisitedMap.put(leftRoot, true);
//		while (!rightQueue.isEmpty()) {
//			ObjectNode node= rightQueue.poll();
//			System.out.println(node.getGlobalId() +"  -  ");
//			Map<JaversProperty, FMCEdge> edges = ((FMCObjectNode)node).getEdges();
//			
//			if(edges != null && !edges.isEmpty())
//			for (Entry<JaversProperty, FMCEdge> entry : edges.entrySet()) {
//				System.out.println("-------------" +entry.getKey().getName()+" : "+entry.getKey().getType()+" : "+entry.getValue());
//				
//				FMCEdge edge = entry.getValue();
//				if(edge instanceof FMCShallowSingleEdge){
//					System.out.println("shallow single edge " + edge);
//					
//					//queue.offer(((FMCShallowSingleEdge)edge).ge )
//				}else if(edge instanceof FMCSingleEdge){
//					//System.out.println("single edge " + edge);
//					ObjectNode childNode = ((FMCSingleEdge) edge).getReferenceNode();
//					//if(nodeVisitedMap.get(childNode) == null || !nodeVisitedMap.get(childNode)){
//					rightQueue.offer((FMCObjectNode) childNode);
//					  // nodeVisitedMap.put((FMCObjectNode)childNode, true);
//					  // }
//				}else if(edge instanceof FMCMultiEdge){
//					//System.out.println("multi edge " + edge);
//					List<ObjectNode> references =((FMCMultiEdge)edge).getReferences();
//					if(references != null && !references.isEmpty())
//					for (ObjectNode childNode : ((FMCMultiEdge)edge).getReferences()) {
//						//if(nodeVisitedMap.get(childNode) == null || !nodeVisitedMap.get(childNode)){
//						rightQueue.offer((FMCObjectNode) childNode);
//						 //  nodeVisitedMap.put((FMCObjectNode)childNode, true);
//						  // }
//					}
//				}
//			}
//			
//		}
		
		
		
		
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

	private ChangeEntry getDiff(String propertyName, FMCObjectNode leftRoot, FMCObjectNode rightRoot,
			Map<GlobalId, Map<Class, Map<String, Change>>> diffMap, Map<GlobalId, Change> newObjectChangeMap,
			Map<GlobalId, Change> objectRemovedChangeMap, GlobalId parentGlobalId) {
		
		Map<JaversProperty, FMCEdge> leftNodeEdges = ((FMCObjectNode)leftRoot).getEdges();
		Map<JaversProperty, FMCEdge> rightNodeEdges = ((FMCObjectNode)rightRoot).getEdges();
		ObjectChangeEntry objectChangeEntry = new ObjectChangeEntry(propertyName);
		

		
		// TODO data property need to process 
		GlobalId leftGlobalId = leftRoot.getGlobalId();
		GlobalId rightGlobalId = rightRoot.getGlobalId();
		if (leftGlobalId instanceof InstanceId && rightGlobalId instanceof InstanceId) {
			if (leftGlobalId.equals(rightGlobalId)) {
				Map<String, Change> propertyChangeMap = diffMap.get(leftGlobalId).get(ValueChange.class);
				for (Entry<String, Change> entry : propertyChangeMap.entrySet()) {
					ValueChange change = (ValueChange) entry.getValue();
					ValueChangeEntry valueChangeEntry = new ValueChangeEntry(entry.getKey(),new Value(change.getLeft()), new Value(change.getRight()));
					objectChangeEntry.addPropertyChangeEntry(entry.getKey(), valueChangeEntry);
				}

		// TODO

				if (leftNodeEdges == null || leftNodeEdges.isEmpty()) {

				} else if (rightNodeEdges == null || rightNodeEdges.isEmpty()) {

				} else {

					Set<JaversProperty> leftNodeEdgeKeySet = leftNodeEdges.keySet();
					Set<JaversProperty> rightNodeEdgeKeySet = rightNodeEdges.keySet();

					Set<JaversProperty> commonNodeEdgeKeySet = Sets.intersection(leftNodeEdgeKeySet,
							rightNodeEdgeKeySet);
					for (JaversProperty javersProperty : commonNodeEdgeKeySet) {
						FMCEdge leftNodeEdge = leftNodeEdges.get(javersProperty);
						FMCEdge rightNodeEdge = rightNodeEdges.get(javersProperty);
						objectChangeEntry.addPropertyChangeEntry(javersProperty.getName(),
								(PropertyChangeEntry) proccessCommonEdge(diffMap, newObjectChangeMap,
										objectRemovedChangeMap, leftNodeEdge, rightNodeEdge, parentGlobalId));
					}
					Set<JaversProperty> onlyLeftNodeEdgeKeySet = Sets.difference(leftNodeEdgeKeySet,
							rightNodeEdgeKeySet);
					for (JaversProperty javersProperty : onlyLeftNodeEdgeKeySet) {
						FMCEdge leftNodeEdge = leftNodeEdges.get(javersProperty);
						// FMCEdge rightNodeEdge =
						// leftNodeEdges.get(javersProperty);
						proccessOnlyLeftEdge(objectChangeEntry, diffMap, newObjectChangeMap, objectRemovedChangeMap,
								leftNodeEdge);
					}
					Set<JaversProperty> onlyRightNodeEdgeKeySet = Sets.difference(rightNodeEdgeKeySet,
							leftNodeEdgeKeySet);
					for (JaversProperty javersProperty : onlyRightNodeEdgeKeySet) {
						// FMCEdge leftNodeEdge =
						// leftNodeEdges.get(javersProperty);
						FMCEdge rightNodeEdge = leftNodeEdges.get(javersProperty);
						proccessOnlyRightEdge(objectChangeEntry, diffMap, newObjectChangeMap, objectRemovedChangeMap,
								rightNodeEdge);
					}

				}

			} else {
				objectChangeEntry.addPropertyChangeEntry(propertyName, (PropertyChangeEntry) getJaversDiff(leftRoot.getCdo().getWrappedCdo(), rightRoot.getCdo().getWrappedCdo()));
				//Map<String, Change> propertyChangeMap = diffMap.get(parentGlobalId).get(ReferenceChange.class);
				//for (Entry<String, Change> entry : propertyChangeMap.entrySet()) {
//					ReferenceChange change = (ReferenceChange) entry.getValue();
//					GlobalId left= change.getLeft();
//					GlobalId right= change.getRight();
					
					
					
					
//					ValueChangeEntry valueChangeEntry = new ValueChangeEntry(entry.getKey(),new Value(change.getLeft()), new Value(change.getRight()));
//					objectChangeEntry.addPropertyChangeEntry(entry.getKey(), valueChangeEntry);
			//	}
			}
		}
		
		return null;
	}

	private ChangeEntry getJaversDiff(Object left, Object right) {
		Javers javers = FMCJaversBuilder.javers().build();
		return ((FMCJaversCore)javers).compareAndConvert(left, right);
	}

	private ChangeEntry proccessOnlyRightEdge(ChangeEntry rootChangeEntry,
			Map<GlobalId, Map<Class, Map<String, Change>>> diffMap, Map<GlobalId, Change> newObjectChangeMap,
			Map<GlobalId, Change> objectRemovedChangeMap, FMCEdge rightNodeEdge) {
		// TODO Auto-generated method stub
		return null;
		
	}

	private ChangeEntry proccessOnlyLeftEdge(ChangeEntry rootChangeEntry,
			Map<GlobalId, Map<Class, Map<String, Change>>> diffMap, Map<GlobalId, Change> newObjectChangeMap,
			Map<GlobalId, Change> objectRemovedChangeMap, FMCEdge leftNodeEdge) {
		
		
		// TODO Auto-generated method stub
		return null;
	}

	private ChangeEntry proccessCommonEdge(Map<GlobalId, Map<Class, Map<String, Change>>> diffMap,
			Map<GlobalId, Change> newObjectChangeMap, Map<GlobalId, Change> objectRemovedChangeMap,
			FMCEdge leftNodeEdge, FMCEdge rightNodeEdge, GlobalId parentGlobalId) {
		
		if (leftNodeEdge != null && rightNodeEdge != null) {
			
			if (leftNodeEdge instanceof FMCSingleEdge) {
				String propertyName = leftNodeEdge.getProperty().getName();
				ObjectChangeEntry changeEntry = new ObjectChangeEntry(propertyName);
				FMCObjectNode leftEdgeReference = (FMCObjectNode) ((FMCSingleEdge) leftNodeEdge).getReferenceNode();
				FMCObjectNode rightEdgeReference = (FMCObjectNode) ((FMCSingleEdge) rightNodeEdge).getReferenceNode();
				
				return getDiff(propertyName, leftEdgeReference, rightEdgeReference, diffMap, newObjectChangeMap,
						objectRemovedChangeMap, parentGlobalId);
			} else if (leftNodeEdge instanceof FMCMultiEdge) {

			}
		}
		return null;
	}
}
