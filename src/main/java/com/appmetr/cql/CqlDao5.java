package com.appmetr.cql;

import com.appmetr.cql.util.CallbackExecutorFactory;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.MappingManager;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class CqlDao5<T, P1, P2, P3, P4, P5> extends CqlDao<T> {

    public CqlDao5(MappingManager mappingManager, Class<T> entityClass) {
        super(mappingManager, entityClass);
    }

    public CqlDao5(MappingManager mappingManager, Class<T> entityClass, CallbackExecutorFactory callbackExecutorFactory) {
        super(mappingManager, entityClass, callbackExecutorFactory);
    }

    public CqlDao5(MappingManager mappingManager, Class<T> entityClass, Executor callbackExecutor) {
        super(mappingManager, entityClass, callbackExecutor);
    }

    @Nullable
    public T get(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5) {
        return getSingleBy(p1, p2, p3, p4, p5);
    }

    public CompletableFuture<T> getAsync(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5) {
        return getAsyncSingleBy(p1, p2, p3, p4, p5);
    }

    public void deleteOne(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5) {
        deleteBy(p1, p2, p3, p4, p5);
    }

    public CompletableFuture<Void> deleteAsyncOne(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5) {
        return deleteAsyncBy(p1, p2, p3, p4, p5);
    }

    public Select.Where getQuery(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5) {
        return getQueryBy(p1, p2, p3, p4, p5);
    }
}
