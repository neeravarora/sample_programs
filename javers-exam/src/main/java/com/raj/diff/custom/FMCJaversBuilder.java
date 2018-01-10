package com.raj.diff.custom;

import static org.javers.common.reflection.ReflectionUtil.isClassPresent;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.javers.core.ConditionalTypesPlugin;
import org.javers.core.Javers;
import org.javers.core.JaversBuilder;
import org.javers.core.JaversCoreConfiguration;
import org.javers.core.diff.DiffFactoryModule;
import org.javers.core.graph.CollectionsCdoFactory;
import org.javers.core.graph.GraphFactoryModule;
import org.javers.core.graph.LiveCdoFactory;
import org.javers.core.graph.LiveGraphFactory;
import org.javers.core.graph.TailoredJaversMemberFactoryModule;
import org.javers.core.metamodel.type.JaversType;
import org.javers.core.pico.AddOnsModule;
import org.javers.core.pico.JaversModule;
import org.javers.groovysupport.GroovyAddOns;
import org.javers.guava.GuavaAddOns;
import org.javers.jodasupport.JodaAddOns;
import org.picocontainer.MutablePicoContainer;

import com.raj.diff.custom.graph.FMCGraphFactoryModule;
import com.raj.diff.custom.graph.FMCTailoredJaversMemberFactoryModule;

public class FMCJaversBuilder extends JaversBuilder{
    private final Set<ConditionalTypesPlugin> conditionalTypesPlugins;
	
	protected FMCJaversBuilder() {

		        conditionalTypesPlugins = new HashSet<>();

		        if (isClassPresent("groovy.lang.MetaClass")) {
		            conditionalTypesPlugins.add(new GroovyAddOns());
		        }
		        if (isClassPresent("org.joda.time.LocalDate")){
		            conditionalTypesPlugins.add(new JodaAddOns());
		        }
		        if (isClassPresent("com.google.common.collect.Multimap")) {
		            conditionalTypesPlugins.add(new GuavaAddOns());
		        }

		        bootContainer();
		        addModule(new FMCCoreJaversModule(getContainer()));
		    
	}
	
	 public static JaversBuilder javers() {
		 FMCJaversBuilder javersBuilder =  new FMCJaversBuilder();
		 javersBuilder.removeComponent(DiffFactoryModule.class);
		 javersBuilder.addModule(new FMCDiffFactoryModule());
	        return javersBuilder;
	    }

	protected void addModule(JaversModule module ) {
		super.addModule(module);
	}
	
	protected void removeComponent(Class<DiffFactoryModule> module ) {
		super.removeComponent(module);
	}
	
	@Override
	protected Javers assembleJaversInstance() {
		super.assembleJaversInstance();
//		this.removeComponent(LiveCdoFactory.class);
//		this.removeComponent(CollectionsCdoFactory.class);
//		this.removeComponent(LiveGraphFactory.class);
    this.removeComponent(GraphFactoryModule.class);
    this.removeComponent(TailoredJaversMemberFactoryModule.class);
		this.addModule(new FMCGraphFactoryModule(this.getContainer()));
		this.addModule(new FMCTailoredJaversMemberFactoryModule(coreConfiguration(), getContainer()));
		return getContainerComponent(FMCJaversCore.class);
	}
	
	  protected MutablePicoContainer getContainer() {
	        return super.getContainer();
	    }
	  
	  private JaversCoreConfiguration coreConfiguration() {
	        return getContainerComponent(JaversCoreConfiguration.class);
	    }
	  
	   private Set<JaversType> bootAddOns() {
	        Set<JaversType> additionalTypes = new HashSet<>();

	        for (ConditionalTypesPlugin plugin : conditionalTypesPlugins) {

	            plugin.beforeAssemble(this);

	            additionalTypes.addAll(plugin.getNewTypes());

	            AddOnsModule addOnsModule = new AddOnsModule(getContainer(), (Collection)plugin.getPropertyChangeAppenders());
	            addModule(addOnsModule);
	        }

	        return additionalTypes;
	    }

}
