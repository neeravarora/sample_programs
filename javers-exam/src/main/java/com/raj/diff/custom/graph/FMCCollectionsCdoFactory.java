package com.raj.diff.custom.graph;

import org.javers.common.collections.Lists;
import org.javers.common.reflection.JaversMember;
import org.javers.core.metamodel.object.Cdo;
import org.javers.core.metamodel.object.CdoWrapper;
import org.javers.core.metamodel.object.UnboundedValueObjectId;
import org.javers.core.metamodel.property.Property;
import org.javers.core.metamodel.scanner.ClassScanner;
import org.javers.core.metamodel.type.JaversProperty;
import org.javers.core.metamodel.type.TypeMapper;
import org.javers.core.metamodel.type.ValueObjectType;

public class FMCCollectionsCdoFactory {

    private final ClassScanner classScanner;
    private final FMCTailoredJaversMemberFactory memberGenericTypeInjector;
    private final TypeMapper typeMapper;

    public FMCCollectionsCdoFactory(ClassScanner classScanner, FMCTailoredJaversMemberFactory memberGenericTypeInjector, TypeMapper typeMapper) {
        this.classScanner = classScanner;
        this.memberGenericTypeInjector = memberGenericTypeInjector;
        this.typeMapper = typeMapper;
    }

    public Cdo createCdo(final FMCCollectionWrapper wrapper, final Class<?> clazz) {
        Property primaryProperty = classScanner.scan(wrapper.getClass()).getProperties().get(0);
        JaversMember javersMember = memberGenericTypeInjector.create(primaryProperty, clazz);

        Property fixedProperty = new Property(javersMember, false);
        JaversProperty fixedJProperty = new JaversProperty(() -> typeMapper.getPropertyType(fixedProperty), fixedProperty);

        ValueObjectType valueObject = new ValueObjectType(wrapper.getClass(), Lists.asList(fixedJProperty));
        return new CdoWrapper(wrapper, new UnboundedValueObjectId(valueObject.getName()), valueObject);
    }
}
