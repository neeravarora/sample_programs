package com.raj.diff.custom.graph;

import org.javers.core.metamodel.object.Cdo;

public class FMCCollectionsGraphBuilder  {//FMCCollectionsGraphBuilder
    private final FMCCollectionsCdoFactory collectionsCdoFactory;
    private final FMCObjectGraphBuilder objectGraphBuilder;

    public FMCCollectionsGraphBuilder(FMCObjectGraphBuilder objectGraphBuilder, FMCCollectionsCdoFactory collectionsCdoFactory) {
        this.collectionsCdoFactory = collectionsCdoFactory;
        this.objectGraphBuilder = objectGraphBuilder;
    }

    public FMCLiveGraph buildGraph(FMCCollectionWrapper wrappedCollection, final Class clazz) {
        Cdo cdo = collectionsCdoFactory.createCdo(wrappedCollection, clazz);

        return objectGraphBuilder.buildGraphFromCdo(cdo);
    }
}
