package com.appmetr.cql.util;

@FunctionalInterface
public interface BiObjLongConsumer<T, U> {

    void accept(T t, U u, long value);
}
