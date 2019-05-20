package com.datastax.driver.mapping;

import com.datastax.driver.core.ConsistencyLevel;

import java.util.stream.Stream;

import static com.datastax.driver.mapping.AnnotationParser.parseEntity;

public class EntityMapperExposer<T> {
    private final EntityMapper<T> entityMapper;

    public EntityMapperExposer(EntityMapper<T> entityMapper) {this.entityMapper = entityMapper;}

    public static <E> EntityMapperExposer<E> fromMappingManager(MappingManager mappingManager, Class<E> entityClass) {
        EntityMapper<E> entityMapper = parseEntity(entityClass, null, mappingManager);
        return new EntityMapperExposer<>(entityMapper);
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
