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

import java.io.IOException;
import java.util.List;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;

public class RecordFieldFollow implements Follow {
    private FieldType fieldType;

    public RecordFieldFollow(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    @Override
    public void follow(IndexUpdateBuilder indexUpdateBuilder, FollowCallback followCallback) throws RepositoryException, IOException, InterruptedException {
        if (!indexUpdateBuilder.getSystemFields().isSystemField(fieldType.getName())) {
            indexUpdateBuilder.addDependency(fieldType.getId());
        }
        RecordContext ctx = indexUpdateBuilder.getRecordContext();
        if (ctx.record != null) {
            List records = IndexerUtils.flatList(ctx.record, fieldType);
            for (Record record: (List<Record>)records) {
                indexUpdateBuilder.push(record, ctx.contextRecord, ctx.dep); // TODO: pass null instead of ctx.dep?
                followCallback.call();
                indexUpdateBuilder.pop();
            }
        }
    }
}
