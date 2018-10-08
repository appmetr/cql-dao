package com.appmetr.cql.util;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import java.io.Serializable;
import java.util.function.BiFunction;

public abstract class Range<R> implements BiFunction<Select.Where, String, Select.Where>, Serializable {
    private static final long serialVersionUID = -4756353103568973426L;

    public static <T> Range<T> from(T from) {
        return new FromInclusive<>(from);
    }

    public static <T> Range<T> fromInclusive(T from) {
        return new FromInclusive<>(from);
    }

    public static <T> Range<T> fromExclusive(T from) {
        return new FromExclusive<>(from);
    }

    public static <T> Range<T> to(T to) {
        return new ToInclusive<>(to);
    }

    public static <T> Range<T> toInclusive(T to) {
        return new ToInclusive<>(to);
    }

    public static <T> Range<T> toExclusive(T to) {
        return new ToExclusive<>(to);
    }

    public static <T> Range<T> fromTo(T from, T to) {
        return new FromInclusiveToExclusive<>(from, to);
    }

    public static <T> Range<T> fromInclusiveToInclusive(T from, T to) {
        return new FromInclusiveToInclusive<>(from, to);
    }

    public static <T> Range<T> fromInclusiveToExclusive(T from, T to) {
        return new FromInclusiveToExclusive<>(from, to);
    }

    public static <T> Range<T> fromExclusiveToExclusive(T from, T to) {
        return new FromExclusiveToExclusive<>(from, to);
    }

    public static <T> Range<T> fromExclusiveToInclusive(T from, T to) {
        return new FromExclusiveToInclusive<>(from, to);
    }

    protected abstract static class FromOrTo<T> extends Range<T> {
        private static final long serialVersionUID = -2271062096224346172L;

        protected final T value;

        protected FromOrTo(T from) {
            this.value = from;
        }

        @Override public String toString() {
            return getClass().getSimpleName() + "{" +
                    "value=" + value +
                    "} " + super.toString();
        }
    }

    protected static class FromInclusive<T> extends FromOrTo<T> {
        private static final long serialVersionUID = 2704298244981760994L;

        protected FromInclusive(T from) {
            super(from);
        }

        @Override public Select.Where apply(Select.Where where, String name) {
            return where.and(QueryBuilder.gte(name, value));
        }
    }

    protected static class FromExclusive<T> extends FromOrTo<T> {
        private static final long serialVersionUID = -4896032594900680029L;

        protected FromExclusive(T from) {
            super(from);
        }

        @Override public Select.Where apply(Select.Where where, String name) {
            return where.and(QueryBuilder.gt(name, value));
        }
    }

    protected static class ToInclusive<T> extends FromOrTo<T> {
        private static final long serialVersionUID = 5189267737660439790L;

        protected ToInclusive(T to) {
            super(to);
        }

        @Override public Select.Where apply(Select.Where where, String name) {
            return where.and(QueryBuilder.lte(name, value));
        }
    }

    protected static class ToExclusive<T> extends FromOrTo<T> {
        private static final long serialVersionUID = -664960400234140662L;

        protected ToExclusive(T to) {
            super(to);
        }

        @Override public Select.Where apply(Select.Where where, String name) {
            return where.and(QueryBuilder.lt(name, value));
        }
    }

    protected abstract static class FromTo<T> extends Range<T> {
        private static final long serialVersionUID = 5275335226046910838L;

        protected final T from;
        protected final T to;

        protected FromTo(T from, T to) {
            this.from = from;
            this.to = to;
        }

        @Override public String toString() {
            return getClass().getSimpleName() + "{" +
                    "from=" + from +
                    ", to=" + to +
                    "} " + super.toString();
        }
    }

    protected static class FromInclusiveToExclusive<T> extends FromTo<T> {
        private static final long serialVersionUID = 85271127797211687L;

        protected FromInclusiveToExclusive(T from, T to) {
            super(from, to);
        }

        @Override public Select.Where apply(Select.Where where, String name) {
            return where.and(QueryBuilder.gte(name, from)).and(QueryBuilder.lt(name, to));
        }
    }

    protected static class FromInclusiveToInclusive<T> extends FromTo<T> {
        private static final long serialVersionUID = 6376882582594092659L;

        protected FromInclusiveToInclusive(T from, T to) {
            super(from, to);
        }

        @Override public Select.Where apply(Select.Where where, String name) {
            return where.and(QueryBuilder.gte(name, from)).and(QueryBuilder.lte(name, to));
        }
    }

    protected static class FromExclusiveToInclusive<T> extends FromTo<T> {
        private static final long serialVersionUID = 7719257560631296547L;

        protected FromExclusiveToInclusive(T from, T to) {
            super(from, to);
        }

        @Override public Select.Where apply(Select.Where where, String name) {
            return where.and(QueryBuilder.gt(name, from)).and(QueryBuilder.lte(name, to));
        }
    }

    protected static class FromExclusiveToExclusive<T> extends FromTo<T> {
        private static final long serialVersionUID = 4965698136318624849L;

        protected FromExclusiveToExclusive(T from, T to) {
            super(from, to);
        }

        @Override public Select.Where apply(Select.Where where, String name) {
            return where.and(QueryBuilder.gt(name, from)).and(QueryBuilder.lt(name, to));
        }
    }
}
