package com.raj.diff.custom.graph;

import java.lang.reflect.Method;
import java.lang.reflect.Type;

import org.javers.common.reflection.JaversGetter;
import org.javers.core.metamodel.property.Property;

public class FMCTailoredJaversMethodFactory extends FMCTailoredJaversMemberFactory {

    @Override
    public JaversGetter create(final Property primaryProperty, final Class<?> genericItemClass) {
        return new JaversGetter((Method) primaryProperty.getMember().getRawMember(), null) {
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
