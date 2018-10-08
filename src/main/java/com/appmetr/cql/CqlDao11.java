package com.appmetr.cql;

import com.appmetr.cql.util.CallbackExecutorFactory;
import com.appmetr.cql.util.Range;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

public class CqlDao11<T, P1, C1> extends CqlDao2<T, P1, C1> {

    public CqlDao11(MappingManager mappingManager, Class<T> entityClass) {
        super(mappingManager, entityClass);
    }

    public CqlDao11(MappingManager mappingManager, Class<T> entityClass, CallbackExecutorFactory callbackExecutorFactory) {
        super(mappingManager, entityClass, callbackExecutorFactory);
    }

    public CqlDao11(MappingManager mappingManager, Class<T> entityClass, Executor callbackExecutor) {
        super(mappingManager, entityClass, callbackExecutor);
    }

    public Result<T> getResult(P1 p1) {
        return getResultBy(p1);
    }

    public Stream<T> getStream(P1 p1) {
        return getStreamBy(p1);
    }

    public List<T> getList(P1 p1) {
        return getListBy(p1);
    }

    public Select.Where getQuery(P1 p1) {
        return getQueryBy(p1);
    }

    public Result<T> rangeResult(P1 p1, Range<C1> range) {
        return result(rangeQuery(p1, range));
    }

    public Stream<T> rangeStream(P1 p1, Range<C1> range) {
        return resultToStream(rangeResult(p1, range));
    }

    public List<T> rangeList(P1 p1, Range<C1> range) {
        return rangeResult(p1, range).all();
    }

    public Select.Where rangeQuery(P1 p1, Range<C1> range) {
        return range.apply(getQueryBy(p1), tableMeta().getClusteringColumns().get(0).getName());
    }

    @Nullable
    public T getFirst(P1 p1) {
        return getFirstBy(p1);
    }

    public CompletableFuture<T> getAsyncFirst(P1 p1) {
        return getAsyncFirstBy(p1);
    }

    public void deleteAll(P1 p1) {
        deleteBy(p1);
    }

    public CompletableFuture<Void> deleteAsyncAll(P1 p1) {
        return deleteAsyncBy(p1);
    }
}
