package com.raj.diff.custom;

import java.util.Collection;

import org.javers.common.collections.Lists;
import org.javers.core.pico.JaversModule;

public class FMCDiffFactoryModule implements JaversModule{

	    @SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
	    public Collection<Class> getComponents() {
	        return (Collection) Lists.asList(
	                FMCDiffFactory.class
	        );
	    }
}
