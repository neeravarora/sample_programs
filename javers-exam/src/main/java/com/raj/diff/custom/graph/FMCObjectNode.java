package com.raj.diff.custom.graph;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.javers.common.validation.Validate;
import org.javers.core.graph.ObjectNode;
import org.javers.core.metamodel.object.Cdo;
import org.javers.core.metamodel.object.GlobalId;
import org.javers.core.metamodel.property.Property;
import org.javers.core.metamodel.type.JaversProperty;
import org.javers.core.metamodel.type.ManagedType;

import com.google.common.collect.Maps;

public class FMCObjectNode extends ObjectNode{
	
	private final Cdo cdo;
	private final Map<JaversProperty, FMCEdge> edges = new LinkedHashMap<>();

	public FMCObjectNode(Cdo cdo) {
		super(cdo);
		this.cdo = cdo;
	}
	
	/**
     * @return returns {@link Optional#EMPTY} for snapshots
     */
    public Optional<Object> wrappedCdo() {
        return cdo.getWrappedCdo();
    }

    /**
     * shortcut to {@link Cdo#getGlobalId()}
     */
    public GlobalId getGlobalId() {
        return cdo.getGlobalId();
    }

    /**
     * only for properties with return type: ManagedType
     */
    public GlobalId getReference(Property property){
        FMCEdge edge = getEdge(property); //could be null for snapshots

        //TODO this is ugly, how to move this logic to Cdo implementations?
        if (edge instanceof AbstractFMCSingleEdge){
            return ((AbstractFMCSingleEdge)edge).getReference();
        }
        else {
            return (GlobalId)getPropertyValue(property);
        }
    }

    public Object getPropertyValue(Property property) {
        Validate.argumentIsNotNull(property);
        return cdo.getPropertyValue(property);
    }

    public boolean isNull(Property property){
        return cdo.isNull(property);
    }

    FMCEdge getEdge(Property property) {
        return edges.get(property);
    }

    FMCEdge getEdge(String propertyName) {
        for (JaversProperty p :  edges.keySet()){
            if (p.getName().equals(propertyName)){
                return getEdge(p);
            }
        }
        return null;
    }

    void addEdge(FMCEdge edge) {
        this.edges.put(edge.getProperty(), edge);
    }

    public ManagedType getManagedType() {
        return cdo.getManagedType();
    }

    public Cdo getCdo() {
        return cdo;
    }

    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FMCObjectNode that = (FMCObjectNode) o;
        return cdo.equals(that.cdo);
    }

    public int hashCode() {
        return cdo.hashCode();
    }

	public Map<JaversProperty, FMCEdge> getEdges() {
		return Collections.unmodifiableMap(edges);
	}
    
    

}
