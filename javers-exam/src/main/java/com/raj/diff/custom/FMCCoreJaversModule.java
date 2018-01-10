package com.raj.diff.custom;

import java.util.Collection;

import org.javers.common.collections.Lists;
import org.javers.core.JaversCoreConfiguration;
import org.javers.core.json.JsonConverterBuilder;
import org.javers.core.metamodel.object.GlobalIdFactory;
import org.javers.core.pico.InstantiatingModule;
import org.javers.repository.jql.QueryRunner;
import org.javers.repository.jql.ShadowQueryRunner;
import org.picocontainer.MutablePicoContainer;

public class FMCCoreJaversModule extends InstantiatingModule {
    public FMCCoreJaversModule(MutablePicoContainer container) {
        super(container);
    }

    @Override
    protected Collection<Class> getImplementations() {
        return Lists.<Class>asList(
                FMCJaversCore.class,
                JsonConverterBuilder.class,
                JaversCoreConfiguration.class,
                GlobalIdFactory.class,
                QueryRunner.class,
                ShadowQueryRunner.class
        );
    }
}