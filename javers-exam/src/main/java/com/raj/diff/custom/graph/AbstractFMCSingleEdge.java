package com.raj.diff.custom.graph;


import org.javers.core.metamodel.object.GlobalId;
import org.javers.core.metamodel.type.JaversProperty;

abstract class AbstractFMCSingleEdge extends FMCEdge {
	AbstractFMCSingleEdge(JaversProperty property) {
        super(property);
    }

    abstract GlobalId getReference() ;

}
