/*
 * Copyright 2011 Outerthought bvba
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
package org.lilyproject.repository.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TypeBucket {

    private final String bucketId;
    private final List<FieldType> fieldTypes = new ArrayList<FieldType>();
    private final List<RecordType> recordTypes = new ArrayList<RecordType>();

    public TypeBucket(String bucketId) {
        this.bucketId = bucketId;
    }

    public String getBucketId() {
        return bucketId;
    }

    public List<FieldType> getFieldTypes() {
        return fieldTypes;
    }

    public List<RecordType> getRecordTypes() {
        return recordTypes;
    }

    public void add(FieldType fieldType) {
        fieldTypes.add(fieldType);
    }

    public void add(RecordType recordType) {
        recordTypes.add(recordType);
    }

    public void addAll(Collection<RecordType> recordType) {
        recordTypes.addAll(recordType);
    }

}
