package com.raj.diff.custom.graph;

import java.util.Collection;

import org.javers.common.collections.Lists;
import org.javers.core.graph.LiveCdoFactory;
import org.javers.core.pico.InstantiatingModule;
import org.picocontainer.MutablePicoContainer;

public class FMCGraphFactoryModule extends InstantiatingModule {
    public FMCGraphFactoryModule(MutablePicoContainer container) {
        super(container);
    }
    @Override
    protected Collection<Class> getImplementations() {
        return (Collection) Lists.asList(
             //  LiveCdoFactory.class,
               FMCCollectionsCdoFactory.class,
               FMCLiveGraphFactory.class,
               FMCObjectGraphBuilder.class//,
              // FMCObjectAccessHookDoNothingImpl.class
               );
    }

}
