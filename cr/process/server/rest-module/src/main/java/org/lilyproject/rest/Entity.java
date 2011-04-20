package org.lilyproject.rest;

import org.lilyproject.tools.import_.json.WriteOptions;

import javax.ws.rs.core.UriInfo;

/**
 * Wrapper for an entity that combines the entity with options concerning its serialization.
 */
public class Entity<T> {
    private T entity;
    private WriteOptions options;

    public Entity(T entity) {
        this(entity, WriteOptions.INSTANCE);
    }

    public Entity(T entity, WriteOptions options) {
        this.entity = entity;
        this.options = options;
    }

    public T getEntity() {
        return entity;
    }

    public WriteOptions getWriteOptions() {
        return options;
    }

    public static <F> Entity<F> create(F entity) {
        return new Entity<F>(entity);
    }

    public static <F> Entity<F> create(F entity, UriInfo uriInfo) {
        return new Entity<F>(entity, ResourceClassUtil.getWriteOptions(uriInfo));
    }
}
