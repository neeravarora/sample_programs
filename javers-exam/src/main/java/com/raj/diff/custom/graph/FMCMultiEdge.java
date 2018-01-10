package com.raj.diff.custom.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.javers.core.graph.ObjectNode;
import org.javers.core.metamodel.type.JaversProperty;

public class FMCMultiEdge extends FMCEdge{

    private final List<ObjectNode> references; //should not be empty

    public FMCMultiEdge(JaversProperty property) {
        super(property);
        references = new ArrayList<>();
    }

    public List<ObjectNode> getReferences(){
        return Collections.unmodifiableList(references);
    }

    public void addReferenceNode(ObjectNode objectNode) {
        references.add(objectNode);
    }


}
