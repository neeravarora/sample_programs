package com.raj.diff.custom.graph;

import java.util.Set;

import org.javers.core.graph.ObjectNode;
import org.javers.core.metamodel.object.CdoWrapper;

public class FMCLiveGraph extends FMCObjectGraph<CdoWrapper> {
    private final FMCObjectNode root;

    FMCLiveGraph(FMCObjectNode root, Set<FMCObjectNode> nodes) {
        super(nodes);
        this.root = root;
    }

    public ObjectNode root() {
        return root;
    }

}
