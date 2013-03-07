/*
 * Copyright 2010 Outerthought bvba
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

import java.util.List;

import org.lilyproject.repository.api.MutationCondition;

public class PostAction<T> {
    private String action;
    private T entity;
    // MutationConditions are only relevant for records. If the post-envelope would be extended
    // with many more attributes, we could think of splitting it up according to application.
    private List<MutationCondition> conditions;

    public PostAction(String action, T entity) {
        this.action = action;
        this.entity = entity;
    }

    public PostAction(String action, T entity, List<MutationCondition> conditions) {
        this.action = action;
        this.entity = entity;
        this.conditions = conditions;
    }

    public String getAction() {
        return action;
    }

    public T getEntity() {
        return entity;
    }

    public List<MutationCondition> getConditions() {
        return conditions;
    }
}
