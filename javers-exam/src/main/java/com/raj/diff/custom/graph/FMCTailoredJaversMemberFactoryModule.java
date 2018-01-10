package com.raj.diff.custom.graph;

import java.util.Collection;

import org.javers.common.collections.Lists;
import org.javers.core.JaversCoreConfiguration;
import org.javers.core.MappingStyle;
import org.javers.core.pico.LateInstantiatingModule;
import org.picocontainer.MutablePicoContainer;

public class FMCTailoredJaversMemberFactoryModule extends LateInstantiatingModule {

    public FMCTailoredJaversMemberFactoryModule(JaversCoreConfiguration configuration, MutablePicoContainer container) {
        super(configuration, container);
    }

    @Override
    protected Collection<Class> getImplementations() {
        MappingStyle mappingStyle = getConfiguration().getMappingStyle();

        if (mappingStyle == MappingStyle.BEAN) {
            return (Collection) Lists.asList(FMCTailoredJaversMethodFactory.class);
        } else if (mappingStyle == MappingStyle.FIELD) {
            return (Collection) Lists.asList(FMCTailoredJaversFieldFactory.class);
        } else {
            throw new RuntimeException("not implementation for " + mappingStyle);
        }
    }
}