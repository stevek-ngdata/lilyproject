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

import java.util.List;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.util.repo.SystemFields;
import org.lilyproject.util.repo.VTaggedRecord;

public class ForEachNode extends ContainerMappingNode {

    private final SystemFields systemFields;
    private final Follow follow;
    private final FieldType fieldType;

    public ForEachNode(SystemFields systemFields, Follow follow) {
        this.systemFields = systemFields;
        this.follow = follow;

        if (follow instanceof LinkFieldFollow) {
            fieldType = ((LinkFieldFollow)follow).getFieldType();
        } else if (follow instanceof RecordFieldFollow) {
            fieldType = ((RecordFieldFollow)follow).getFieldType();
        } else {
            fieldType = null;
        }

    }

    public Follow getFollow() {
        return follow;
    }

    @Override
    public boolean isIndexAffectedByUpdate(VTaggedRecord vtRecord, Scope scope) throws InterruptedException,
            RepositoryException {
        if (fieldType != null && !systemFields.isSystemField(fieldType.getName())) {
            return vtRecord.getUpdatedFieldsByScope().get(scope).contains(fieldType.getId());
        }

        return false;
    }

    @Override
    public void collectIndexUpdate(IndexUpdateBuilder indexUpdateBuilder, Record record, long version, SchemaId vtag)
            throws InterruptedException, RepositoryException {
        if (fieldType != null && !systemFields.isSystemField(fieldType.getName())) {
            if (!record.hasField(fieldType.getName())) {
                return;
            }
        }

        collectFromRecords(indexUpdateBuilder, record, version, vtag, indexUpdateBuilder.evalFollow(follow));
    }

    private void collectFromRecords(IndexUpdateBuilder indexUpdateBuilder, Record record, long version, SchemaId vtag, List<FollowRecord> links)
            throws InterruptedException, RepositoryException {
        if (links == null || links.size() == 0)
            return;

        for (FollowRecord followRecord: links) {
            RecordContext ctx = indexUpdateBuilder.getRecordContext();
            ctx.push(followRecord);
            for (MappingNode child : getChildren()) {
                child.collectIndexUpdate(indexUpdateBuilder, followRecord.record, version, vtag);
            }
            ctx.pop();
        }

    }

}
