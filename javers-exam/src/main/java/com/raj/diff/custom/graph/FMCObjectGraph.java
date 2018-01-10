package com.raj.diff.custom.graph;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.javers.core.diff.ObjectGraph;
import org.javers.core.graph.ObjectNode;
import org.javers.core.metamodel.object.Cdo;
import org.javers.core.metamodel.object.GlobalId;

public class FMCObjectGraph<T extends Cdo> extends ObjectGraph<T> {
private final Set<FMCObjectNode> nodes;

public FMCObjectGraph(Set<FMCObjectNode> nodes) {
	super(null);
    this.nodes = nodes;
}

public Set<ObjectNode> nodes() {
	Set<ObjectNode> nodes = new HashSet<>();
	for (FMCObjectNode fmcObjectNode : this.nodes) {
		nodes.add(fmcObjectNode);
	}
    return nodes;
}

public Set<T> cdos() {
    return nodes().stream().map(node -> (T) node.getCdo()).collect(Collectors.toSet());
}

public Set<GlobalId> globalIds() {
    return nodes().stream().map(ObjectNode::getGlobalId).collect(Collectors.toSet());
}

public Optional<T> get(GlobalId globalId) {
    return nodes.stream()
        .filter(node -> globalId.equals(node.getGlobalId()))
        .findFirst()
        .map(node -> (T) node.getCdo());
}


}
