package com.raj.diff.custom.graph;

import org.javers.core.graph.ObjectAccessHook;

public class FMCObjectAccessHookDoNothingImpl implements ObjectAccessHook {
    @Override
    public <T> T access(T entity) {
        return entity;
    }
}
