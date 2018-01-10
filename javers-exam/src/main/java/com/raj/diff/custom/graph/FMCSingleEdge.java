package com.raj.diff.custom.graph;

import org.javers.common.validation.Validate;
import org.javers.core.graph.ObjectNode;
import org.javers.core.metamodel.object.GlobalId;
import org.javers.core.metamodel.type.JaversProperty;

public class FMCSingleEdge extends AbstractFMCSingleEdge {
    private final ObjectNode referencedNode;

    FMCSingleEdge(JaversProperty property, ObjectNode referencedNode) {
        super(property);
        Validate.argumentsAreNotNull(referencedNode);
        this.referencedNode = referencedNode;
    }

    @Override
    public GlobalId getReference() {
        return referencedNode.getGlobalId();
    }
    
    public ObjectNode getReferenceNode() {
        return referencedNode;
    }


}
