package com.appmetr.cql.util;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public interface CallbackExecutorFactory {
    Executor directExecutor = Runnable::run;
    Executor forkJoinExecutor = ForkJoinPool.commonPool();

    CallbackExecutorFactory directFactory = aClass -> directExecutor;
    CallbackExecutorFactory forkJoinFactory = aClass -> forkJoinExecutor;

    Executor createCallbackExecutor(Class aClass);
}
