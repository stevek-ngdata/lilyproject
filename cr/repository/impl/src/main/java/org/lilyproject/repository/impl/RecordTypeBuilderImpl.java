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
package org.lilyproject.repository.impl;

import org.lilyproject.repository.api.*;

public class RecordTypeBuilderImpl implements RecordTypeBuilder {

    private final TypeManager typeManager;
    private RecordType recordType;
    
    public RecordTypeBuilderImpl(TypeManager typeManager) throws TypeException {
        this.typeManager = typeManager;
        recordType = typeManager.newRecordType(null);
    }

    public RecordTypeBuilder name(QName name) {
        recordType.setName(name);
        return this;
    }

    public RecordTypeBuilder id(SchemaId id) {
        recordType.setId(id);
        return this;
    }
    
    public RecordTypeBuilder field(SchemaId id, boolean mandatory) {
        recordType.addFieldTypeEntry(id, mandatory);
        return this;
    }


    public RecordTypeBuilder reset() throws TypeException {
        recordType = typeManager.newRecordType(null);
        return this;
    }

    public RecordType newRecordType() {
        return recordType;
    }

    public RecordType create() throws RepositoryException, InterruptedException {
        return typeManager.createRecordType(recordType);
    }

    public RecordType update() throws RepositoryException, InterruptedException {
        return typeManager.updateRecordType(recordType);
    }

}
