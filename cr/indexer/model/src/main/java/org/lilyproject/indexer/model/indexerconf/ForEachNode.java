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

import java.util.ArrayList;
import java.util.List;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
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
        } else if (follow instanceof LinkFieldFollow) {
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

        if (follow instanceof RecordFieldFollow) {
            collectFromRecords(indexUpdateBuilder, record, version, vtag, (List<Record>)indexUpdateBuilder.evalFollow(follow), true);
        } else {
            collectFromLinks(indexUpdateBuilder, record, version, vtag, (List<Link>)indexUpdateBuilder.evalFollow(follow));
        }
    }

    private void collectFromRecords(IndexUpdateBuilder indexUpdateBuilder, Record record, long version, SchemaId vtag, List<Record> records, boolean embedded)
            throws InterruptedException, RepositoryException {
        for (Record childRecord: records) {
            if (embedded) {
                indexUpdateBuilder.getRecordContext().pushEmbedded(childRecord);
            } else {
                indexUpdateBuilder.getRecordContext().push(childRecord);
            }
            for (MappingNode child : getChildren()) {
                child.collectIndexUpdate(indexUpdateBuilder, childRecord, version, vtag);
            }
            indexUpdateBuilder.getRecordContext().pop();
        }
    }

    private void collectFromLinks(IndexUpdateBuilder indexUpdateBuilder, Record record, long version, SchemaId vtag, List<Link> links)
            throws InterruptedException, RepositoryException {
        if (links.size() == 0) return;

        Repository repository = indexUpdateBuilder.getRepository();
        List<RecordId> recordIds = new ArrayList<RecordId>(links.size());
        for (Link link: links) {
            recordIds.add(link.resolve(record, repository.getIdGenerator()));
        }

        List<Record> records = repository.read(recordIds);
        collectFromRecords(indexUpdateBuilder, record, version, vtag, records, false);
    }

}
