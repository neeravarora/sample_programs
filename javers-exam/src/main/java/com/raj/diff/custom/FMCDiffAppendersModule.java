package com.raj.diff.custom;

import java.util.Collection;

import org.javers.common.collections.Lists;
import org.javers.core.JaversCoreConfiguration;
import org.javers.core.diff.appenders.CollectionChangeFakeAppender;
import org.javers.core.diff.appenders.CorePropertyChangeAppender;
import org.javers.core.diff.appenders.DiffAppendersModule;
import org.javers.core.diff.appenders.OptionalChangeAppender;
import org.javers.core.diff.changetype.container.ListChange;
import org.picocontainer.MutablePicoContainer;

public class FMCDiffAppendersModule extends DiffAppendersModule{

	public FMCDiffAppendersModule(JaversCoreConfiguration javersCoreConfiguration, MutablePicoContainer container) {
		super(javersCoreConfiguration, container);
		this.listChangeAppender = javersCoreConfiguration.getListCompareAlgorithm().getAppenderClass();
	    
	}
	
	  private final Class<? extends CorePropertyChangeAppender<ListChange>> listChangeAppender;

	
	    @Override
	    protected Collection<Class> getImplementations() {
	        return (Collection)Lists.asList(
	        		NewObjectAppender.class,
	                MapChangeAppender.class,
	                listChangeAppender,
	                SetChangeAppender.class,
	                ArrayChangeAppender.class,
	                ObjectRemovedAppender.class,
	                ReferenceChangeAppender.class,
	                OptionalChangeAppender.class,
	                ValueChangeAppender.class,
	                CollectionChangeFakeAppender.class
	        );}

}
