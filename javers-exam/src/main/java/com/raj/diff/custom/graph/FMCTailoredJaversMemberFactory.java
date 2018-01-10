package com.raj.diff.custom.graph;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.javers.common.collections.Lists;
import org.javers.common.reflection.JaversMember;
import org.javers.core.metamodel.property.Property;
import org.javers.core.metamodel.type.ParametrizedDehydratedType;

abstract class FMCTailoredJaversMemberFactory {

    protected abstract JaversMember create(Property primaryProperty, Class<?> genericItemClass);

    protected ParameterizedType parametrizedType(Property property, Class<?> itemClass) {
         return new ParametrizedDehydratedType(property.getRawType(), Lists.asList((Type) itemClass));
    }
}
