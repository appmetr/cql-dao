package com.appmetr.cql;

import com.appmetr.cql.util.*;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.jmmo.util.BreakException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CqlDao<T> {
    public static final int DEFAULT_BATCH_SIZE = 1000;
    public static final UUID MIN_UUID = UUID.fromString("00000000-0000-1000-8080-808080808080");
    public static final UUID MAX_UUID = UUID.fromString("ffffffff-ffff-1fff-bf7f-7f7f7f7f7f7f");
    
    private final MappingManager mappingManager;
    private final Class<T> entityClass;
    private final Mapper<T> mapper;
    private final EntityMapperExposer<T> entityMapperExposer;
    private final Executor callbackExecutor;

    public CqlDao(MappingManager mappingManager, Class<T> entityClass) {
        this(mappingManager, entityClass, CallbackExecutorFactory.directExecutor);
    }

    public CqlDao(MappingManager mappingManager, Class<T> entityClass, CallbackExecutorFactory callbackExecutorFactory) {
        this(mappingManager, entityClass, callbackExecutorFactory.createCallbackExecutor(entityClass));
    }

    public CqlDao(MappingManager mappingManager, Class<T> entityClass, Executor callbackExecutor) {
        this.mappingManager = mappingManager;
        this.entityClass = entityClass;
        this.mapper = mappingManager.mapper(entityClass);
        this.entityMapperExposer = EntityMapperExposer.fromMapper(mapper);
        this.callbackExecutor = callbackExecutor;
    }

    public ResultSet execute(Statement statement) {
        return session().execute(statement);
    }

    public CompletableFuture<ResultSet> executeAsync(Statement statement) {
        return completableFuture(session().executeAsync(statement));
    }

    public CompletableFuture<ResultSet> executeAsync(Statement statement, Executor executor) {
        return completableFuture(session().executeAsync(statement), executor);
    }

    public Flux<Row> executeFlux(Statement statement) {
        return executeFlux(statement, callbackExecutor());
    }

    public Flux<Row> executeFlux(Statement statement, Executor executor) {
        return Mono.fromFuture(executeAsync(statement)).flux().flatMap(result -> resultToFlux(result, executor));
    }

    public Result<T> result(Statement statement) {
        return mapper.map(execute(statement));
    }

    public CompletableFuture<Result<T>> resultAsync(Statement statement) {
        return executeAsync(statement).thenApply(resultSet -> mapper().map(resultSet));
    }

    public Flux<T> resultFlux(Statement statement) {
        return resultFlux(statement, callbackExecutor());
    }

    public Flux<T> resultFlux(Statement statement, Executor executor) {
        return Mono.fromFuture(resultAsync(statement)).flux().flatMap(result -> resultToFlux(result, executor));
    }

    public Stream<T> stream(Statement statement) {
        return resultToStream(result(statement));
    }

    public CompletableFuture<Stream<T>> streamAsync(Statement statement) {
        return resultAsync(statement).thenApply(CqlDao::resultToStream);
    }

    public List<T> list(Statement statement) {
        return result(statement).all();
    }

    public CompletableFuture<List<T>> listAsync(Statement statement) {
        return resultAsync(statement).thenCompose(this::listAsync);
    }

    public CompletableFuture<List<T>> listAsync(Statement statement, Executor executor) {
        return resultAsync(statement).thenCompose(result -> listAsync(result, executor));
    }

    public CompletableFuture<List<T>> listAsync(Result<T> result) {
        return listAsync(result, callbackExecutor());
    }

    public static <T> CompletableFuture<List<T>> listAsync(Result<T> result, Executor executor) {
        final List<T> list = new ArrayList<>();
        return processAsync(result, objLongConsumer(list::add), executor).thenApply(aVoid -> list);
    }

    public long processAll(ObjLongConsumer<T> tConsumer) {
        return processAll(DEFAULT_BATCH_SIZE, tConsumer);
    }

    public long processAll(int batchSize, ObjLongConsumer<T> tConsumer) {
        return process(result(select().setFetchSize(batchSize)), tConsumer);
    }

    public static <R extends PagingIterable<R, T>, T> long process(R result, ObjLongConsumer<T> eConsumer) {
        return processInner(result, () -> null, biObjLongConsumer(eConsumer), b -> {});
    }

    public <R extends PagingIterable<R, T>> CompletableFuture<Long> processAsync(R result, ObjLongConsumer<T> eConsumer) {
        return processAsync(result, eConsumer, callbackExecutor());
    }

    public static <R extends PagingIterable<R, T>, T> CompletableFuture<Long> processAsync(R result, ObjLongConsumer<T> eConsumer, Executor executor) {
        return processAsyncInner(result, eConsumer, executor);
    }

    public static <R extends PagingIterable<R, T>, T, B> long batching(R result, Supplier<B> bSupplier,
                                                                       BiObjLongConsumer<T, B> eConsumer, Consumer<B> bConsumer) {
        return processInner(result, bSupplier, eConsumer, bConsumer);
    }

    public <R extends PagingIterable<R, T>, B> CompletableFuture<Long> batchingAsync(R result, Supplier<B> bSupplier,
                                                                                     BiObjLongConsumer<T, B> eConsumer, Consumer<B> bConsumer) {
        return batchingAsync(result, bSupplier, eConsumer, bConsumer, callbackExecutor());
    }

    public static <R extends PagingIterable<R, T>, T, B> CompletableFuture<Long> batchingAsync(
            R result, Supplier<B> bSupplier, BiObjLongConsumer<T, B> eConsumer, Consumer<B> bConsumer, Executor executor) {
        return batchingAsyncInner(result, bSupplier, eConsumer, bConsumer, executor);
    }

    protected static <R extends PagingIterable<R, T>, T, B> CompletableFuture<Long> batchingAsyncInner(
            R result, Supplier<B> bSupplier, BiObjLongConsumer<T, B> eConsumer, Consumer<B> bConsumer, Executor executor) {
        final CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        processAsyncInner(result, executor, 0L, completableFuture, bSupplier, eConsumer, bConsumer);
        return completableFuture;
    }

    protected static <R extends PagingIterable<R, T>, T, B> long processInner(
            R result, Supplier<B> batchSupplier, BiObjLongConsumer<T, B> eConsumer, Consumer<B> batchConsumer) {

        final long[] count = new long[1];

        while (!result.isExhausted()) {
            try {
                processBatch(result, batchSupplier, eConsumer, batchConsumer, count);
            } catch (BreakException e) {
                break;
            }
        }

        return count[0];
    }

    protected static <R extends PagingIterable<R, T>, T, B> void processBatch(
            R result, Supplier<B> batchSupplier, BiObjLongConsumer<T, B> eConsumer, Consumer<B> batchConsumer, long[] count) {
        final B batch = batchSupplier.get();
        Stream.generate(result::one).limit(result.getAvailableWithoutFetching())
                .forEach(element -> eConsumer.accept(element, batch, ++count[0]));
        batchConsumer.accept(batch);
    }

    protected static <R extends PagingIterable<R, T>, T>  CompletableFuture<Long> processAsyncInner(
            R result, ObjLongConsumer<T> eConsumer, Executor executor) {
        final CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        processAsyncInner(result, executor, 0L, completableFuture, () -> null, biObjLongConsumer(eConsumer), aVoid -> {});
        return completableFuture;
    }

    protected static <R extends PagingIterable<R, E>, E, B> void processAsyncInner(
            R result, Executor executor, long previousCount, CompletableFuture<Long> cf,
            Supplier<B> batchSupplier, BiObjLongConsumer<E, B> eConsumer, Consumer<B> batchConsumer) {
        executor.execute(() -> {
            try {
                final long[] count = new long[] {previousCount};
                try {
                    if (result.getAvailableWithoutFetching() > 0) {
                        processBatch(result, batchSupplier, eConsumer, batchConsumer, count);
                    }
                } catch (BreakException e) {
                    cf.complete(count[0]);
                    return;
                }

                if (result.getExecutionInfo().getPagingState() == null) {
                    cf.complete(count[0]);
                } else {
                    Futures.addCallback(result.fetchMoreResults(), new FutureCallback<R>() {
                        @Override public void onSuccess(R result) {
                            processAsyncInner(result, executor, count[0], cf, batchSupplier, eConsumer, batchConsumer);
                        }
                        @Override public void onFailure(Throwable t) {
                            cf.completeExceptionally(t);
                        }
                    }, executor);
                }
            } catch (Throwable throwable) {
                cf.completeExceptionally(throwable);
            }
        });
    }

    public Class<T> entityClass() {
        return entityClass;
    }

    public Mapper<T> mapper() {
        return mapper;
    }

    public EntityMapperExposer<T> entityMapper() {
        return entityMapperExposer;
    }

    public MappingManager mappingManager() {
        return mappingManager;
    }

    public Session session() {
        return mappingManager().getSession();
    }

    public TableMetadata tableMeta() {
        return mapper().getTableMetadata();
    }

    public Executor callbackExecutor() {
        return callbackExecutor;
    }

    public Select select() {
        return QueryBuilder.select().from(tableMeta());
    }

    public Select select(Object... columns) {
        return QueryBuilder.select(columns).from(tableMeta());
    }

    @Nullable
    public T getSingleBy(Object... primaryKey) {
        return mapper.get(primaryKey);
    }

    public CompletableFuture<T> getAsyncSingleBy(Object... primaryKey) {
        return completableFuture(mapper.getAsync(primaryKey));
    }

    public Result<T> getAllResult() {
        return result(select());
    }

    public List<T> getAllList() {
        return getAllResult().all();
    }

    public Stream<T> getAllStream() {
        return resultToStream(getAllResult());
    }

    public ResultSet getAllPartitionKeys() {
        return session().execute(getAllPartitionKeysQuery());
    }

    public Select getAllPartitionKeysQuery() {
        final Select.Selection selection = QueryBuilder.select().distinct();
        tableMeta().getPartitionKey().forEach(columnMetadata -> selection.column(columnMetadata.getName()));
        return selection.from(tableMeta());
    }

    public Select.Where getQueryBy(Object... keys) {
        Select.Selection selection = QueryBuilder.select();
        for (Iterator<MappedProperty> it = entityMapper().getAllColumns().iterator(); it.hasNext(); ) {
            final MappedProperty mappedProperty = it.next();
            selection = mappedProperty.isComputed() ? selection.raw(mappedProperty.getMappedName()) : selection.column(mappedProperty.getMappedName());
        }

        final Select.Where where = selection.from(tableMeta()).where();
        final Iterator<MappedProperty> primaryKeys = entityMapper().getPrimaryKeys().iterator();
        IntStream.range(0, keys.length).forEach(i -> where.and(QueryBuilder.eq(primaryKeys.next().getMappedName(), keys[i])));

        return where;
    }

    public Result<T> getResultBy(Object... keys) {
        return result(getQueryBy(keys));
    }

    public Stream<T> getStreamBy(Object... keys) {
        return resultToStream(getResultBy(keys));
    }

    public List<T> getListBy(Object... keys) {
        return getResultBy(keys).all();
    }

    @Nullable
    public T getFirstBy(Object... keys) {
        return getResultBy(keys).one();
    }

    public CompletableFuture<T> getAsyncFirstBy(Object... keys) {
        return resultAsync(getQueryBy(keys)).thenApply(Result::one);
    }

    public void save(T t) {
        prePersist(t);
        mapper().save(t);
        postPersist(t);
    }

    public void save(T t, Mapper.Option... options) {
        prePersist(t);
        mapper().save(t, options);
        postPersist(t);
    }

    public void save(T t, int ttlSeconds) {
        save(t, Mapper.Option.ttl(ttlSeconds));
    }

    public void save(T t, Predicate<String> fieldFilter) {
        execute(saveStatement(t, fieldFilter));
        postPersist(t);
    }

    protected CompletableFuture<Void> saveAsyncInner(T t, Supplier<ListenableFuture<Void>> listenableFutureSupplier) {
        prePersist(t);
        return completableFuture(listenableFutureSupplier.get()).thenRun(() -> postPersist(t));
    }

    public CompletableFuture<Void> saveAsync(T t) {
        return saveAsyncInner(t, () -> mapper().saveAsync(t));
    }

    public CompletableFuture<Void> saveAsync(T t, Mapper.Option... options) {
        return saveAsyncInner(t, () -> mapper().saveAsync(t, options));
    }

    public CompletableFuture<Void> saveAsync(T t, int ttlSeconds) {
        return saveAsync(t, Mapper.Option.ttl(ttlSeconds));
    }

    /**
     * The method is not completely asynchronous if statement is not prepared yet
     */
    public CompletableFuture<Void> saveAsync(T t, Predicate<String> fieldFilter) {
        return executeAsync(saveStatement(t, fieldFilter)).thenRun(() -> postPersist(t));
    }

    protected Statement saveStatement(T t, Predicate<String> fieldFilter, Mapper.Option... options) {
        final ProtocolVersion protocolVersion = session().getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
        if (protocolVersion.compareTo(ProtocolVersion.V4) < 0) {
            throw new IllegalStateException("Cluster protocol version " + protocolVersion + " does not support unset in BoundStatement");
        }

        prePersist(t);

        final Statement statement = mapper().saveQuery(t, options);
        if (!(statement instanceof BoundStatement)) {
            throw new IllegalStateException("Mapper.saveQuery() expected to return BoundStatement but returns " + statement);
        }
        final BoundStatement boundStatement = (BoundStatement) statement;

        final List<ColumnMetadata> primaryKey = tableMeta().getPrimaryKey();
        tableMeta().getColumns().stream()
                .filter(columnMetadata -> !primaryKey.contains(columnMetadata))
                .map(ColumnMetadata::getName)
                .filter(fieldFilter.negate())
                .filter(name -> boundStatement.preparedStatement().getVariables().contains(name))
                .forEach(boundStatement::unset);

        return boundStatement;
    }

    public void save(T t, String... nonKeyedFields) {
        save(t, fieldName -> Stream.of(nonKeyedFields).anyMatch(fieldName::equalsIgnoreCase));
    }

    /**
     * The method is not completely asynchronous if statement is not prepared yet
     */
    public CompletableFuture<Void> saveAsync(T t, String... nonKeyedFields) {
        return saveAsync(t, fieldName -> Stream.of(nonKeyedFields).anyMatch(fieldName::equalsIgnoreCase));
    }

    public void delete(T t) {
        preDelete(t);
        mapper().delete(t);
        postDelete(t);
    }

    public CompletableFuture<Void> deleteAsync(T t) {
        preDelete(t);
        return completableFuture(mapper().deleteAsync(t)).thenAccept(aVoid -> postDelete(t));
    }

    /**
     * preDelete and postDelete will not execute here
     */
    public void deleteBy(Object key, Object... keys) {
        execute(deleteQueryBy(key, keys));
    }

    /**
     * The method is not completely asynchronous if statement is not prepared yet
     */
    public CompletableFuture<Void> deleteAsyncBy(Object key, Object... keys) {
        return completableFuture(session().executeAsync(deleteQueryBy(key, keys))).thenApply(rows -> null);
    }

    public Delete.Where deleteQueryBy(Object key, Object... keys) {
        final List<ColumnMetadata> primaryKey = tableMeta().getPrimaryKey();
        final Delete.Where where = QueryBuilder.delete().from(tableMeta()).where(QueryBuilder.eq(primaryKey.get(0).getName(), key));
        IntStream.range(0, keys.length).forEach(i -> where.and(QueryBuilder.eq(primaryKey.get(i + 1).getName(), keys[i])));
        return where;
    }

    public static <R extends PagingIterable<R, T>, T> Stream<T> resultToStream(R result) {
        return StreamSupport.stream(result.spliterator(), false);
    }


    public static <R extends PagingIterable<R, T>, T> Flux<T> resultToFlux(R result) {
        return resultToFlux(result, ForkJoinPool.commonPool());
    }

    public static <R extends PagingIterable<R, T>, T> Flux<T> resultToFlux(R result, Executor executor) {
        return Flux.create(sink -> {
            final PagingIterableEmitter<R, T> fluxSkeleton = new PagingIterableEmitter<>(result, sink::next, sink::complete, sink::error, executor);
            sink.onRequest(fluxSkeleton::request);
            sink.onCancel(fluxSkeleton::dispose);
            sink.onDispose(fluxSkeleton::dispose);
        });
    }

    public Flux<Select.Where> scanQuery(int ranges, Supplier<Select> selectSupplier) {
        final long[] tokens = MurMur64Tokens.tokens(ranges);
        return Flux.range(0, ranges)
                .map(i -> MurMur64Tokens.whereTokenRange(this, tokens, i, selectSupplier.get()));
    }

    public ParallelFlux<T> scan(int ranges) {
        return scan(ranges, ForkJoinPool.commonPool(), this::select);
    }

    public ParallelFlux<T> scan(int ranges, Executor executor, Supplier<Select> select) {
        return scanQuery(ranges, select).parallel(ranges).flatMap(query -> resultFlux(query, executor));
    }

    public ParallelFlux<Row> scanRow(int ranges) {
        return scanRow(ranges, this::select);
    }

    public ParallelFlux<Row> scanRow(int ranges, Supplier<Select> selectSupplier) {
        return scanRow(ranges, selectSupplier, ForkJoinPool.commonPool());
    }

    public ParallelFlux<Row> scanRow(int ranges, Supplier<Select> selectSupplier, Executor executor) {
        return scanQuery(ranges, selectSupplier).parallel(ranges).flatMap(query -> executeFlux(query, executor));
    }

    public <R> CompletableFuture<R> completableFuture(ListenableFuture<R> listenableFuture) {
        return completableFuture(listenableFuture, callbackExecutor());
    }

    public static <R> CompletableFuture<R> completableFuture(ListenableFuture<R> listenableFuture, Executor executor) {
        return new CompletableListenable<>(listenableFuture, executor);
    }

    public static <T> ObjLongConsumer<T> objLongConsumer(Consumer<T> tConsumer) {
        return (t, value) -> tConsumer.accept(t);
    }

    public static <T> BiObjLongConsumer<T, Void> biObjLongConsumer(ObjLongConsumer<T> objLongConsumer) {
        return (t, o, i) -> objLongConsumer.accept(t, i);
    }

    public static <T> BiObjLongConsumer<T, Void> biObjLongConsumer(Consumer<T> tConsumer) {
        return (t, o, i) -> tConsumer.accept(t);
    }

    protected void prePersist(T t) {}

    protected void postPersist(T t) {}

    protected void preDelete(T t) {}

    protected void postDelete(T t) {}
}
