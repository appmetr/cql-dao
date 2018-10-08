package com.appmetr.cql;

import com.appmetr.cql.util.CallbackExecutorFactory;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.MappingManager;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class CqlDao1<T, P1> extends CqlDao<T> {

    public CqlDao1(MappingManager mappingManager, Class<T> entityClass) {
        super(mappingManager, entityClass);
    }

    public CqlDao1(MappingManager mappingManager, Class<T> entityClass, CallbackExecutorFactory callbackExecutorFactory) {
        super(mappingManager, entityClass, callbackExecutorFactory);
    }

    public CqlDao1(MappingManager mappingManager, Class<T> entityClass, Executor callbackExecutor) {
        super(mappingManager, entityClass, callbackExecutor);
    }

    @Nullable
    public T get(P1 p1) {
        return getSingleBy(p1);
    }

    public CompletableFuture<T> getAsync(P1 p1) {
        return getAsyncSingleBy(p1);
    }

    public void deleteOne(P1 p1) {
        deleteBy(p1);
    }

    public CompletableFuture<Void> deleteAsyncOne(P1 p1) {
        return deleteAsyncBy(p1);
    }

    public Select.Where getQuery(P1 p1) {
        return getQueryBy(p1);
    }
}
