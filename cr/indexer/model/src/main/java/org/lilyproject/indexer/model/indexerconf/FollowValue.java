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
package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.SchemaId;

/**
 * FollowValues are used when a dereferencing value ends with a 'variant follow' (master, -prop*, (+prop|+prop=value)* )
 * They are not used for LinkFieldFollow and RecordFieldFollow (those are handled using 'FieldValue' objects)
 */
public class FollowValue extends BaseValue {
    private Follow follow;

    protected FollowValue(Follow follow, boolean extractContent, String formatter) {
        super(extractContent, formatter);
        this.follow = follow;
    }

    public FieldType getFieldType() {
        return null;
    }

    @Override
    public SchemaId getFieldDependency() {
        return null;
    }

    @Override
    public FieldType getTargetFieldType() {
        return null;
    }

}
