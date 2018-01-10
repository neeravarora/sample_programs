package com.raj.diff.custom.graph;

import org.javers.common.validation.Validate;
import org.javers.core.metamodel.type.JaversProperty;

abstract public class FMCEdge {
    private final JaversProperty property;

    FMCEdge(JaversProperty property) {
        Validate.argumentIsNotNull(property);
        this.property = property;
    }

    public JaversProperty getProperty() {
        return property;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        FMCEdge that = (FMCEdge) obj;
        return property.equals(that.property);
    }

    @Override
    public int hashCode() {
        return property.hashCode();
    }


}
