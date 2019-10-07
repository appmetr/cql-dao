package com.appmetr.cql.util;

import com.appmetr.cql.CqlDao;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.MappedProperty;

import java.math.BigInteger;
import java.util.stream.IntStream;

public class MurMur64Tokens {
    private MurMur64Tokens() {}

    public static long[] tokens(int n) {
        return IntStream.range(0, n)
                .mapToLong(i -> token(i, n))
                .toArray();
    }

    public static long token(int i, int n) {
        return BigInteger.valueOf(2).pow(64).divide(BigInteger.valueOf(n)).multiply(BigInteger.valueOf(i)).subtract(BigInteger.valueOf(2).pow(63)).longValue();
    }

    public static <T> Select.Where whereTokenRange(CqlDao<T> dao, long[] tokens, int i, Select select) {
        final String tokenFunc = QueryBuilder.token(dao.entityMapper().getPartitionKeys().map(MappedProperty::getMappedName).toArray(String[]::new));
        return select
                .where(QueryBuilder.gte(tokenFunc, tokens[i]))
                .and(QueryBuilder.lte(tokenFunc, i == tokens.length - 1 ? Long.MAX_VALUE : tokens[i + 1] - 1));
    }
}
