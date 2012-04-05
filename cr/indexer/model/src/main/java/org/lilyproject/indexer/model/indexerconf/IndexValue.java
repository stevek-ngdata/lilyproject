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
package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdRecord;

/**
 * A value to index, together with the coordinates where it came from.
 */
public class IndexValue {
    public IdRecord record;
    public FieldType fieldType;
    public Integer listIndex;
    public Object value;

    public IndexValue(IdRecord record, FieldType fieldType, Integer listIndex, Object value) {
        this.record = record;
        this.fieldType = fieldType;
        this.listIndex = listIndex;
        this.value = value;
    }

    public IndexValue(IdRecord record, FieldType fieldType, Object value) {
        this(record, fieldType, null, value);
    }
}
