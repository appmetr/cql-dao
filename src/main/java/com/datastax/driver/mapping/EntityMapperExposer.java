package com.datastax.driver.mapping;

import com.datastax.driver.core.ConsistencyLevel;
import org.jmmo.util.StreamUtil;

import java.lang.reflect.Field;
import java.util.stream.Stream;

public class EntityMapperExposer<T> {
    private final EntityMapper<T> entityMapper;

    public EntityMapperExposer(EntityMapper<T> entityMapper) {this.entityMapper = entityMapper;}

    @SuppressWarnings("unchecked")
    public static <E> EntityMapperExposer<E> fromMapper(Mapper<E> mapper) {
        try {
            final Field field = mapper.getClass().getDeclaredField("mapper");
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }
            return new EntityMapperExposer<>((EntityMapper<E>) field.get(mapper));
        } catch (IllegalAccessException | NoSuchFieldException  e) {
            return StreamUtil.sneakyThrow(e);
        }
    }

    public String getKeyspace() {
        return entityMapper.keyspace;
    }

    public String getTable() {
        return entityMapper.table;
    }

    public ConsistencyLevel getWriteConsistency() {
        return entityMapper.writeConsistency;
    }

    public ConsistencyLevel getReadConsistency() {
        return entityMapper.readConsistency;
    }

    public Stream<MappedProperty> getPartitionKeys() {
        return entityMapper.partitionKeys.stream().map(aliasedMappedProperty -> aliasedMappedProperty.mappedProperty);
    }

    public Stream<MappedProperty> getClusteringColumns() {
        return entityMapper.clusteringColumns.stream().map(aliasedMappedProperty -> aliasedMappedProperty.mappedProperty);
    }

    public Stream<MappedProperty> getAllColumns() {
        return entityMapper.allColumns.stream().map(aliasedMappedProperty -> aliasedMappedProperty.mappedProperty);
    }

    public Stream<MappedProperty> getPrimaryKeys() {
        return Stream.concat(entityMapper.partitionKeys.stream(), entityMapper.clusteringColumns.stream())
                .map(aliasedMappedProperty -> aliasedMappedProperty.mappedProperty);
    }
}
