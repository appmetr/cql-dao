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

public class CqlDao32<T, P1, P2, P3, C1, C2> extends CqlDao5<T, P1, P2, P3, C1, C2> {

    public CqlDao32(MappingManager mappingManager, Class<T> entityClass) {
        super(mappingManager, entityClass);
    }

    public CqlDao32(MappingManager mappingManager, Class<T> entityClass, CallbackExecutorFactory callbackExecutorFactory) {
        super(mappingManager, entityClass, callbackExecutorFactory);
    }

    public CqlDao32(MappingManager mappingManager, Class<T> entityClass, Executor callbackExecutor) {
        super(mappingManager, entityClass, callbackExecutor);
    }

    public Result<T> getResult(P1 p1, P2 p2, P3 p3) {
        return getResultBy(p1, p2, p3);
    }

    public Stream<T> getStream(P1 p1, P2 p2, P3 p3) {
        return getStreamBy(p1, p2, p3);
    }

    public List<T> getList(P1 p1, P2 p2, P3 p3) {
        return getResult(p1, p2, p3).all();
    }

    public Select.Where getQuery(P1 p1, P2 p2, P3 p3) {
        return getQueryBy(p1, p2, p3);
    }

    public Result<T> rangeResult(P1 p1, P2 p2, P3 p3, Range<C1> range) {
        return result(rangeQuery(p1, p2, p3, range));
    }

    public Stream<T> rangeStream(P1 p1, P2 p2, P3 p3, Range<C1> range) {
        return resultToStream(rangeResult(p1, p2, p3, range));
    }

    public List<T> rangeList(P1 p1, P2 p2, P3 p3, Range<C1> range) {
        return rangeResult(p1, p2, p3, range).all();
    }

    public Select.Where rangeQuery(P1 p1, P2 p2, P3 p3, Range<C1> range) {
        return range.apply(getQueryBy(p1, p2, p3), tableMeta().getClusteringColumns().get(0).getName());
    }

    @Nullable
    public T getFirst(P1 p1, P2 p2, P3 p3) {
        return getFirstBy(p1, p2, p3);
    }

    public CompletableFuture<T> getAsyncFirst(P1 p1, P2 p2, P3 p3) {
        return getAsyncFirstBy(p1, p2, p3);
    }

    public void deleteAll(P1 p1, P2 p2, P3 p3) {
        deleteBy(p1, p2, p3);
    }

    public CompletableFuture<Void> deleteAsyncAll(P1 p1, P2 p2, P3 p3) {
        return deleteAsyncBy(p1, p2, p3);
    }

    public Result<T> getResult(P1 p1, P2 p2, P3 p3, C1 c1) {
        return getResultBy(p1, p2, p3, c1);
    }

    public Stream<T> getStream(P1 p1, P2 p2, P3 p3, C1 c1) {
        return getStreamBy(p1, p2, p3, c1);
    }

    public List<T> getList(P1 p1, P2 p2, P3 p3, C1 c1) {
        return getListBy(p1, p2, p3, c1);
    }

    public Select.Where getQuery(P1 p1, P2 p2, P3 p3, C1 c1) {
        return getQueryBy(p1, p2, p3, c1);
    }

    public Result<T> rangeResult(P1 p1, P2 p2, P3 p3, C1 c1, Range<C2> range) {
        return result(rangeQuery(p1, p2, p3, c1, range));
    }

    public Stream<T> rangeStream(P1 p1, P2 p2, P3 p3, C1 c1, Range<C2> range) {
        return resultToStream(rangeResult(p1, p2, p3, c1, range));
    }

    public List<T> rangeList(P1 p1, P2 p2, P3 p3, C1 c1, Range<C2> range) {
        return rangeResult(p1, p2, p3, c1, range).all();
    }

    public Select.Where rangeQuery(P1 p1, P2 p2, P3 p3, C1 c1, Range<C2> range) {
        return range.apply(getQueryBy(p1, p2, p3, c1), tableMeta().getClusteringColumns().get(1).getName());
    }

    @Nullable
    public T getFirst(P1 p1, P2 p2, P3 p3, C1 c1) {
        return getFirstBy(p1, p2, p3, c1);
    }

    public CompletableFuture<T> getAsyncFirst(P1 p1, P2 p2, P3 p3, C1 c1) {
        return getAsyncFirstBy(p1, p2, p3, c1);
    }

    public void deleteAll(P1 p1, P2 p2, P3 p3, C1 c1) {
        deleteBy(p1, p2, p3, c1);
    }

    public CompletableFuture<Void> deleteAsyncAll(P1 p1, P2 p2, P3 p3, C1 c1) {
        return deleteAsyncBy(p1, p2, p3, c1);
    }
}
