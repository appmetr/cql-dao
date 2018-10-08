package com.appmetr.cql.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

public class LazyCallbackExecutorFactory implements CallbackExecutorFactory {
    private final List<LazyExecutor> executors = new ArrayList<>();
    private final long keepAliveMs;

    public LazyCallbackExecutorFactory() {
        this(0L);
    }

    public LazyCallbackExecutorFactory(long keepAliveMs) {
        this.keepAliveMs = keepAliveMs;
    }

    @Override public Executor createCallbackExecutor(Class aClass) {
        final LazyExecutor lazyExecutor = new LazyExecutor(() -> {
            ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1, 1, keepAliveMs,
                    TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> new Thread(r, "cql-cb-" + aClass.getSimpleName()));
            threadPoolExecutor.allowCoreThreadTimeOut(keepAliveMs > 0);
            return threadPoolExecutor;
        });
        executors.add(lazyExecutor);
        return lazyExecutor;
    }

    public void shutdown() {
        executors.stream()
                .peek(lazyExecutor -> lazyExecutor.setShutdown(true))
                .map(LazyExecutor::getExecutor)
                .filter(Objects::nonNull)
                .forEach(executor -> ((ExecutorService) executor).shutdown());
    }
}
