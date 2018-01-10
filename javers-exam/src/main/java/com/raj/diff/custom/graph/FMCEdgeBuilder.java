package com.raj.diff.custom.graph;

import org.javers.common.collections.EnumerableFunction;
import org.javers.core.graph.CdoFactory;
import org.javers.core.graph.ObjectNode;
import org.javers.core.metamodel.object.Cdo;
import org.javers.core.metamodel.object.EnumerationAwareOwnerContext;
import org.javers.core.metamodel.object.OwnerContext;
import org.javers.core.metamodel.object.PropertyOwnerContext;
import org.javers.core.metamodel.type.ContainerType;
import org.javers.core.metamodel.type.EnumerableType;
import org.javers.core.metamodel.type.JaversProperty;
import org.javers.core.metamodel.type.KeyValueType;
import org.javers.core.metamodel.type.ManagedType;
import org.javers.core.metamodel.type.TypeMapper;

public class FMCEdgeBuilder {
    private final TypeMapper typeMapper;
    private final FMCNodeReuser nodeReuser;
    private final CdoFactory cdoFactory;

    FMCEdgeBuilder(TypeMapper typeMapper, FMCNodeReuser nodeReuser, CdoFactory cdoFactory) {
        this.typeMapper = typeMapper;
        this.nodeReuser = nodeReuser;
        this.cdoFactory = cdoFactory;
    }

    String graphType(){
        return cdoFactory.typeDesc();
    }

    /**
     * @return node stub, could be redundant so check reuse context
     */
    AbstractFMCSingleEdge buildSingleEdge(ObjectNode node, JaversProperty singleRef) {
        Object rawReference = node.getPropertyValue(singleRef);
        Cdo cdo = cdoFactory.create(rawReference, createOwnerContext(node, singleRef));

        if (!singleRef.isShallowReference()){
            ObjectNode targetNode = buildNodeStubOrReuse(cdo);
            return new FMCSingleEdge(singleRef, targetNode);
        }
        return new FMCShallowSingleEdge(singleRef, cdo);
    }

    private OwnerContext createOwnerContext(ObjectNode parentNode, JaversProperty property) {
        return new PropertyOwnerContext(parentNode.getGlobalId(), property.getName());
    }

    FMCMultiEdge createMultiEdge(JaversProperty containerProperty, EnumerableType enumerableType, ObjectNode node) {
    	FMCMultiEdge multiEdge = new FMCMultiEdge(containerProperty);
        OwnerContext owner = createOwnerContext(node, containerProperty);

        Object container = node.getPropertyValue(containerProperty);

        EnumerableFunction edgeBuilder = null;
        if (enumerableType instanceof KeyValueType){
            edgeBuilder = new MultiEdgeMapBuilderFunction(multiEdge);
        } else if (enumerableType instanceof ContainerType){
            edgeBuilder = new MultiEdgeContainerBuilderFunction(multiEdge);
        }
        enumerableType.map(container, edgeBuilder, owner);

        return multiEdge;
    }

    private class MultiEdgeContainerBuilderFunction implements EnumerableFunction {
        private final FMCMultiEdge multiEdge;

        public MultiEdgeContainerBuilderFunction(FMCMultiEdge multiEdge) {
            this.multiEdge = multiEdge;
        }

        @Override
        public Object apply(Object input, EnumerationAwareOwnerContext context) {
            if (!isManagedPosition(input)){
                return input;
            }
            ObjectNode objectNode = buildNodeStubOrReuse(cdoFactory.create(input, context));
            multiEdge.addReferenceNode(objectNode);
            return input;
        }

        boolean isManagedPosition(Object input){
            return true;
        }
    }

    private class MultiEdgeMapBuilderFunction extends MultiEdgeContainerBuilderFunction {
        public MultiEdgeMapBuilderFunction(FMCMultiEdge multiEdge) {
            super(multiEdge);
        }

        boolean isManagedPosition(Object input){
            return typeMapper.getJaversType(input.getClass()) instanceof ManagedType;
        }
    }

    private ObjectNode buildNodeStubOrReuse(Cdo cdo){
        if (nodeReuser.isReusable(cdo)){
            return nodeReuser.getForReuse(cdo);
        }
        else {
            return buildNodeStub(cdo);
        }
    }

    FMCObjectNode buildNodeStub(Cdo cdo){
        FMCObjectNode newStub = new FMCObjectNode(cdo);
        nodeReuser.enqueueStub(newStub);
        return newStub;
    }
}
