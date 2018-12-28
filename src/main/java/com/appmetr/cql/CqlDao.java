package com.appmetr.cql;

import com.appmetr.cql.util.CallbackExecutorFactory;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.jmmo.util.BreakException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.ObjLongConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
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

    public Result<T> result(Statement statement) {
        return mapper.map(execute(statement));
    }

    public CompletableFuture<Result<T>> resultAsync(Statement statement) {
        return executeAsync(statement).thenApply(resultSet -> mapper().map(resultSet));
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

    public <E> CompletableFuture<List<E>> listAsync(Result<E> result) {
        return listAsync(result, callbackExecutor());
    }

    public <E> CompletableFuture<List<E>> listAsync(Result<E> result, Executor executor) {
        final List<E> list = new ArrayList<>();
        return processAsync(result, objLongConsumer(list::add), executor).thenApply(aVoid -> list);
    }

    public <E> CompletableFuture<Long> processAsync(Result<E> result, ObjLongConsumer<E> eConsumer) {
        return processAsync(result, eConsumer, callbackExecutor());
    }

    public <E> CompletableFuture<Long> processAsync(Result<E> result, ObjLongConsumer<E> eConsumer, Executor executor) {
        final CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        processAsyncInner(result, executor, 0L, completableFuture, eConsumer);
        return completableFuture;
    }

    public CompletableFuture<Long> processRowAsync(ResultSet result, ObjLongConsumer<Row> rConsumer) {
        return processRowAsync(result, rConsumer, callbackExecutor());
    }

    public CompletableFuture<Long> processRowAsync(ResultSet result, ObjLongConsumer<Row> rConsumer, Executor executor) {
        final CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        processAsyncInner(result, executor, 0L, completableFuture, rConsumer);
        return completableFuture;
    }

    protected <S extends PagingIterable<S, E>, E> void processAsyncInner(PagingIterable<S, E> result, Executor executor, long previousCount,
                                                                         CompletableFuture<Long> cf, ObjLongConsumer<E> eConsumer) {
        executor.execute(() -> {
            try {
                final long[] count = new long[] {previousCount};
                try {
                    Stream.generate(result::one).limit(result.getAvailableWithoutFetching())
                            .forEach(element -> eConsumer.accept(element, ++count[0]));
                } catch (BreakException e) {
                    cf.complete(count[0]);
                    return;
                }

                if (result.getExecutionInfo().getPagingState() == null) {
                    cf.complete(count[0]);
                } else {
                    Futures.addCallback(result.fetchMoreResults(), new FutureCallback<PagingIterable<S, E>>() {
                        @Override public void onSuccess(PagingIterable<S, E> result) {
                            processAsyncInner(result, executor, count[0], cf, eConsumer);
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

    public long processAll(ObjLongConsumer<T> tConsumer) {
        return processAll(DEFAULT_BATCH_SIZE, tConsumer);
    }

    public long processAll(int batchSize, ObjLongConsumer<T> tConsumer) {
        return process(result(select().setFetchSize(batchSize)), tConsumer);
    }

    public <E> long process(Result<E> result, ObjLongConsumer<E> eConsumer) {
        return processInner(result, eConsumer);
    }

    public long processRow(ResultSet result, ObjLongConsumer<Row> eConsumer) {
        return processInner(result, eConsumer);
    }

    protected <S extends PagingIterable<S, E>, E> long processInner(PagingIterable<S, E> result, ObjLongConsumer<E> eConsumer) {
        final long[] count = new long[1];

        while (!result.isExhausted()) {
            try {
                Stream.generate(result::one).limit(result.getAvailableWithoutFetching())
                        .forEach(element -> eConsumer.accept(element, ++count[0]));
            } catch (BreakException e) {
                break;
            }
        }

        return count[0];
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

    public static <T> Stream<T> resultToStream(Result<T> result) {
        return StreamSupport.stream(result.spliterator(), false);
    }

    public static Stream<Row> resultSetToStream(ResultSet resultSet) {
        return StreamSupport.stream(resultSet.spliterator(), false);
    }

    public <R> CompletableFuture<R> completableFuture(ListenableFuture<R> listenableFuture) {
        return completableFuture(listenableFuture, callbackExecutor());
    }

    public static <R> CompletableFuture<R> completableFuture(ListenableFuture<R> listenableFuture, Executor executor) {
        return new CompletableListenable<>(listenableFuture, executor);
    }
    public static class CompletableListenable<T> extends CompletableFuture<T> implements FutureCallback<T> {

        private final ListenableFuture<T> listenableFuture;

        public CompletableListenable(ListenableFuture<T> listenableFuture, Executor executor) {
            this.listenableFuture = listenableFuture;

            Futures.addCallback(listenableFuture, this, executor);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean result = listenableFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
        }

        @Override public void onSuccess(T result) {
            complete(result);
        }
        @Override public void onFailure(Throwable t) {
            completeExceptionally(t);
        }

    }

    public static <T> ObjLongConsumer<T> objLongConsumer(Consumer<T> tConsumer) {
        return (t, value) -> tConsumer.accept(t);
    }

    protected void prePersist(T t) {}

    protected void postPersist(T t) {}

    protected void preDelete(T t) {}

    protected void postDelete(T t) {}
}
