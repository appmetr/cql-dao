package com.appmetr.cql.util;

import com.appmetr.cql.CqlDao;
import com.datastax.driver.core.PagingIterable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class PagingIterableEmitter<R extends PagingIterable<R, T>, T> {
    private final Consumer<T> onNext;
    private final Runnable onCompleted;
    private final Consumer<Throwable> onError;
    private final AtomicLong requested = new AtomicLong();
    private final Executor executor;

    private R pagingIterable;
    private boolean reading;
    private volatile boolean disposed;

    public PagingIterableEmitter(R pagingIterable, Consumer<T> onNext, Runnable onCompleted, Consumer<Throwable> onError, Executor executor) {
        this.pagingIterable = pagingIterable;
        this.onNext = onNext;
        this.onCompleted = onCompleted;
        this.onError = onError;
        this.executor = executor;
    }

    public void request(long n) {
        synchronized (requested) {
            if (requested.getAndAdd(n) > 0 || reading) {
                return;
            }
            reading = true;
        }

        CompletableFuture.runAsync(() -> readAvailable(pagingIterable, new CompletableFuture<>()), executor)
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        onError.accept(throwable);
                    }
                });
    }

    private CompletableFuture<Void> readAvailable(R result, CompletableFuture<Void> cf) {
        try {
            do {
                while (requested.get() > 0 && result.getAvailableWithoutFetching() > 0) {
                    if (disposed) {
                        cf.complete(null);
                        return cf;
                    }
                    onNext.accept(result.one());
                    requested.decrementAndGet();
                }

                synchronized (requested) {
                    if (requested.get() == 0) {
                        reading = false;
                        cf.complete(null);
                        return cf;
                    }
                }
            } while (result.getAvailableWithoutFetching() > 0);

            if (result.getExecutionInfo().getPagingState() == null) {
                onCompleted.run();
                cf.complete(null);
                return cf;
            }

            CqlDao.completableFuture(result.fetchMoreResults(), executor)
                    .thenCompose(r -> readAvailable(r, cf));

        } catch (Throwable throwable) {
            cf.completeExceptionally(throwable);
        }

        return cf;
    }

    public void dispose() {
        disposed = true;
    }
}
