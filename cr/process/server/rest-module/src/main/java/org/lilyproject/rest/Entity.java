/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.rest;

import javax.ws.rs.core.UriInfo;

import org.lilyproject.tools.import_.json.WriteOptions;

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
