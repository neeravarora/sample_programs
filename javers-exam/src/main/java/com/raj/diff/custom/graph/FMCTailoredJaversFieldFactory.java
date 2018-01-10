package com.raj.diff.custom.graph;

import java.lang.reflect.Field;
import java.lang.reflect.Type;

import org.javers.common.reflection.JaversField;
import org.javers.core.metamodel.property.Property;

public class FMCTailoredJaversFieldFactory extends FMCTailoredJaversMemberFactory {

    @Override
    public JaversField create(final Property primaryProperty, final Class<?> genericItemClass) {
        return new JaversField((Field) primaryProperty.getMember().getRawMember(), null) {
            @Override
            public Type getGenericResolvedType() {
                return parametrizedType(primaryProperty, genericItemClass);
            }

            @Override
            protected Type getRawGenericType() {
                return genericItemClass;
            }
        };
    }

}
