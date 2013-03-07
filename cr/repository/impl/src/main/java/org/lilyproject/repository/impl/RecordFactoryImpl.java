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
package org.lilyproject.repository.impl;

import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.TypeManager;

public class RecordFactoryImpl implements RecordFactory {

    private RecordDecoder recordDecoder;

    public RecordFactoryImpl(TypeManager typeManager, IdGenerator idGenerator) {
        recordDecoder = new RecordDecoder(typeManager, idGenerator);
    }

    @Override
    public Record newRecord() {
        return recordDecoder.newRecord();
    }

    @Override
    public Record newRecord(RecordId recordId) {
        return recordDecoder.newRecord(recordId);
    }

}
