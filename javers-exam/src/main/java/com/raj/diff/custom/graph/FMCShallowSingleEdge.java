package com.raj.diff.custom.graph;

import org.javers.common.validation.Validate;
import org.javers.core.metamodel.object.Cdo;
import org.javers.core.metamodel.object.GlobalId;
import org.javers.core.metamodel.type.JaversProperty;

public class FMCShallowSingleEdge extends AbstractFMCSingleEdge {
    private final Cdo reference;

    FMCShallowSingleEdge(JaversProperty property, Cdo referencedObject) {
        super(property);
        Validate.argumentIsNotNull(referencedObject);
        this.reference = referencedObject;
    }

    @Override
    GlobalId getReference() {
        return reference.getGlobalId();
    }

}
