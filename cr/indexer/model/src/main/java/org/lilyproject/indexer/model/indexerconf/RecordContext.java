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

import org.lilyproject.repository.api.Record;

public class RecordContext {

    /**
     * In case of embedded (nested) records, contextRecord contains the real repository record,
     * and record the embedded (id-less) record.
     */
    public final Record contextRecord;
    public final Record record;
    public final Dep dep;

    public RecordContext(Record record, Dep dep) {
        this.contextRecord = record;
        this.record = record;
        this.dep = dep;
    }

    public RecordContext(Record record, Record contextRecord, Dep dep) {
        this.record = record;
        this.contextRecord = contextRecord;
        this.dep = dep;
    }

}
