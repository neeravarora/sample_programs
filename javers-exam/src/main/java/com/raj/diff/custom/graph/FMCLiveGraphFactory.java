package com.raj.diff.custom.graph;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.javers.core.graph.LiveCdoFactory;
import org.javers.core.metamodel.object.Cdo;
import org.javers.core.metamodel.type.TypeMapper;

public class FMCLiveGraphFactory {
	

    private final TypeMapper typeMapper;
    private final LiveCdoFactory liveCdoFactory;
    private final FMCCollectionsCdoFactory collectionsCdoFactory;

    public FMCLiveGraphFactory(TypeMapper typeMapper, LiveCdoFactory liveCdoFactory, FMCCollectionsCdoFactory collectionsCdoFactory) {
        this.typeMapper = typeMapper;
        this.liveCdoFactory = liveCdoFactory;
        this.collectionsCdoFactory = collectionsCdoFactory;
    }

    public FMCLiveGraph createLiveGraph(Collection handle, Class clazz) {
        FMCCollectionWrapper wrappedCollection = (FMCCollectionWrapper) wrapTopLevelContainer(handle);

        return new FMCCollectionsGraphBuilder(new FMCObjectGraphBuilder(typeMapper, liveCdoFactory), collectionsCdoFactory)
                .buildGraph(wrappedCollection, clazz);
    }

    /**
     * delegates to {@link ObjectGraphBuilder#buildGraph(Object)}
     */
    public FMCObjectGraph createLiveGraph(Object handle) {
        Object wrappedHandle = wrapTopLevelContainer(handle);

        return new FMCObjectGraphBuilder(typeMapper, liveCdoFactory).buildGraph(wrappedHandle);
    }

    public Cdo createCdo(Object cdo){
        return liveCdoFactory.create(cdo, null);
    }

    private Object wrapTopLevelContainer(Object handle){
        if (handle instanceof  Map){
            return new MapWrapper((Map)handle);
        }
        if (handle instanceof  List){
            return new ListWrapper((List)handle);
        }
        if (handle instanceof  Set){
            return new SetWrapper((Set)handle);
        }
        if (handle.getClass().isArray()){
            return new ArrayWrapper(convertToObjectArray(handle));
        }
        return handle;
    }

    public static Class getMapWrapperType(){
        return MapWrapper.class;
    }

    public static Class getSetWrapperType(){
        return SetWrapper.class;
    }

    public static Class getListWrapperType(){
        return ListWrapper.class;
    }

    public static Class getArrayWrapperType() {
        return ArrayWrapper.class;
    }

    static class MapWrapper {
        private final Map<Object,Object> map;

        MapWrapper(Map map) {
            this.map = map;
        }
    }

    static class SetWrapper implements FMCCollectionWrapper {
        private final Set<Object> set;

        SetWrapper(Set set) {
            this.set = set;
        }

        Set<Object> getSet() {
            return set;
        }
    }

    static class ListWrapper implements FMCCollectionWrapper {
        private final List<Object> list;

        ListWrapper(List list) {
            this.list = list;
        }

        List<Object> getList() {
            return list;
        }
    }

    static class ArrayWrapper {
        private final Object[] array;

        ArrayWrapper(Object[] objects) {
            this.array = objects;
        }
    }

    //this is primarily used for copying array of primitives to array of objects
    //as there seems be no legal way for casting
    private static Object[] convertToObjectArray(Object obj) {
        if (obj instanceof Object[]) {
            return (Object[]) obj;
        }
        int arrayLength = Array.getLength(obj);
        Object[] retArray = new Object[arrayLength];
        for (int i = 0; i < arrayLength; ++i){
            retArray[i] = Array.get(obj, i);
        }
        return retArray;
    }



}
