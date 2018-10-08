package com.appmetr.cql.util;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;

public class LazyExecutor implements Executor {

    private Supplier<? extends Executor> executorSupplier;
    private volatile boolean shutdown;
    private volatile Executor executor;

    public LazyExecutor(Supplier<? extends Executor> executorSupplier) {
        this.executorSupplier = executorSupplier;
    }

    @Override public void execute(Runnable command) {
        if (isShutdown()) {
            throw new RejectedExecutionException("The executor has been shut down");
        }

        if (executor == null) {
            synchronized (this) {
                if (executor == null) {
                    executor = executorSupplier.get();
                    executorSupplier = null;
                }
            }
        }
        executor.execute(command);
    }

    public Executor getExecutor() {
        return executor;
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public void setShutdown(boolean shutdown) {
        this.shutdown = shutdown;
    }
}
