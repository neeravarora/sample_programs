package com.raj.diff.custom;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.javers.common.exception.JaversException;
import org.javers.common.exception.JaversExceptionCode;
import org.javers.core.JaversCoreConfiguration;
import org.javers.core.commit.CommitMetadata;
import org.javers.core.diff.Diff;
import org.javers.core.diff.DiffFactory;
import org.javers.core.diff.appenders.NodeChangeAppender;
import org.javers.core.diff.appenders.PropertyChangeAppender;
import org.javers.core.graph.LiveGraph;
import org.javers.core.graph.LiveGraphFactory;
import org.javers.core.metamodel.type.JaversProperty;
import org.javers.core.metamodel.type.JaversType;
import org.javers.core.metamodel.type.PrimitiveType;
import org.javers.core.metamodel.type.TypeMapper;
import org.javers.core.metamodel.type.ValueType;

import com.raj.diff.custom.graph.FMCEdge;
import com.raj.diff.custom.graph.FMCLiveGraph;
import com.raj.diff.custom.graph.FMCLiveGraphFactory;
import com.raj.diff.custom.graph.FMCObjectNode;
import com.raj.diff.model.ChangeEntry;

public class FMCDiffFactory extends DiffFactory {
	
	private final TypeMapper typeMapper;
	private final List<NodeChangeAppender> nodeChangeAppenders;
	private final List<PropertyChangeAppender> propertyChangeAppender;
	private final FMCLiveGraphFactory graphFactory;
	private final JaversCoreConfiguration javersCoreConfiguration;

	public FMCDiffFactory(TypeMapper typeMapper, List<NodeChangeAppender> nodeChangeAppenders,
			List<PropertyChangeAppender> propertyChangeAppender, FMCLiveGraphFactory graphFactory,
			JaversCoreConfiguration javersCoreConfiguration) {
		super(typeMapper, nodeChangeAppenders, propertyChangeAppender, null, javersCoreConfiguration);
		this.typeMapper = typeMapper;
		this.nodeChangeAppenders = nodeChangeAppenders;
		this.graphFactory = graphFactory;
		this.javersCoreConfiguration = javersCoreConfiguration;

		this.propertyChangeAppender = propertyChangeAppender;

	}
	
	FMCDiffConvertor diffConvertor = new FMCDiffConvertor();
	
	public  <T extends Object> ChangeEntry compareAndConvert(T oldVersion, T currentVersion) {
		FMCLiveGraph leftGraph = buildGraph(oldVersion);
		FMCLiveGraph rightGraph = buildGraph(currentVersion);
		Diff diff = create(leftGraph, rightGraph, Optional.<CommitMetadata>empty());
		diffConvertor.transform((FMCObjectNode)leftGraph.root(), (FMCObjectNode)rightGraph.root(), diff);
		
		
		
		return null;
	}

	public Diff compare(Object oldVersion, Object currentVersion) {
		FMCLiveGraph leftGraph = buildGraph(oldVersion);
		FMCLiveGraph rightGraph = buildGraph(currentVersion);
		Diff diff = create(leftGraph, rightGraph, Optional.<CommitMetadata>empty());
		return diff;
	}

	public <T> Diff compareCollections(Collection<T> oldVersion, Collection<T> currentVersion, Class<T> itemClass) {
		return create(buildGraph(oldVersion, itemClass), buildGraph(currentVersion, itemClass),
				Optional.<CommitMetadata>empty());
	}

	private FMCLiveGraph buildGraph(Collection handle, Class itemClass) {
		return graphFactory.createLiveGraph(handle, itemClass);
	}

	private FMCLiveGraph buildGraph(Object handle) {
		JaversType jType = typeMapper.getJaversType(handle.getClass());
		if (jType instanceof ValueType || jType instanceof PrimitiveType) {
			throw new JaversException(JaversExceptionCode.COMPARING_TOP_LEVEL_VALUES_NOT_SUPPORTED,
					jType.getClass().getSimpleName(), handle.getClass().getSimpleName());
		}
		return (FMCLiveGraph) graphFactory.createLiveGraph(handle);
	}
	    
	    
//		SingleValueObjects singleValueObjects = new SingleValueObjects();
	//	
//		public <T> void constructDiffObj(Diff diff, Class<T> t){
//			
//			for (Field field : t.getFields()) {
//				if(singleValueObjects.isSingleValueObjectTpye(field.getType())){
//					
//				}
//				
//			}
//			
//		}

}
