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

public class CqlDao51<T, P1, P2, P3, P4, P5, C1> extends CqlDao6<T, P1, P2, P3, P4, P5, C1> {

    public CqlDao51(MappingManager mappingManager, Class<T> entityClass) {
        super(mappingManager, entityClass);
    }

    public CqlDao51(MappingManager mappingManager, Class<T> entityClass, CallbackExecutorFactory callbackExecutorFactory) {
        super(mappingManager, entityClass, callbackExecutorFactory);
    }

    public CqlDao51(MappingManager mappingManager, Class<T> entityClass, Executor callbackExecutor) {
        super(mappingManager, entityClass, callbackExecutor);
    }

    public Result<T> getResult(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5) {
        return getResultBy(p1, p2, p3, p4, p5);
    }

    public Stream<T> getStream(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5) {
        return getStreamBy(p1, p2, p3, p4, p5);
    }

    public List<T> getList(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5) {
        return getListBy(p1, p2, p3, p4, p5);
    }

    public Select.Where getQuery(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5) {
        return getQueryBy(p1, p2, p3, p4, p5);
    }

    public Result<T> rangeResult(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, Range<C1> range) {
        return result(rangeQuery(p1, p2, p3, p4, p5, range));
    }

    public Stream<T> rangeStream(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, Range<C1> range) {
        return resultToStream(rangeResult(p1, p2, p3, p4, p5, range));
    }

    public List<T> rangeList(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, Range<C1> range) {
        return rangeResult(p1, p2, p3, p4, p5, range).all();
    }

    public Select.Where rangeQuery(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, Range<C1> range) {
        return range.apply(getQueryBy(p1, p2, p3, p4, p5), tableMeta().getClusteringColumns().get(0).getName());
    }

    @Nullable
    public T getFirst(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5) {
        return getFirstBy(p1, p2, p3, p4, p5);
    }

    public CompletableFuture<T> getAsyncFirst(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5) {
        return getAsyncFirstBy(p1, p2, p3, p4, p5);
    }

    public void deleteAll(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5) {
        deleteBy(p1, p2, p3, p4, p5);
    }

    public CompletableFuture<Void> deleteAsyncAll(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5) {
        return deleteAsyncBy(p1, p2, p3, p4, p5);
    }
}
