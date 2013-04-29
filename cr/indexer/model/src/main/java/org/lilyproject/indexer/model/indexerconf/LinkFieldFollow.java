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
import java.util.Collections;
import java.util.List;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.VersionNotFoundException;
import org.lilyproject.util.repo.VersionTag;

public class LinkFieldFollow implements Follow {
    private FieldType fieldType;
    /**
     * If the link field follow is after one or more record follows, then from
     * the point of view of the link index, the link belongs to the same field
     * as the top-level record field. We keep a reference to that field here.
     */
    private FieldType ownerFieldType;

    public LinkFieldFollow(FieldType fieldType) {
        this.fieldType = fieldType;
        this.ownerFieldType = fieldType;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public FieldType getOwnerFieldType() {
        return ownerFieldType;
    }

    public void setOwnerFieldType(FieldType ownerFieldType) {
        this.ownerFieldType = ownerFieldType;
    }

    @Override
    public void follow(IndexUpdateBuilder indexUpdateBuilder, FollowCallback callback) throws RepositoryException, IOException, InterruptedException {
        if (!indexUpdateBuilder.getSystemFields().isSystemField(fieldType.getName())) {
            indexUpdateBuilder.addDependency(fieldType.getId());
        }
        IdGenerator idGenerator = indexUpdateBuilder.getRepositoryManager().getIdGenerator();

        RecordContext ctx = indexUpdateBuilder.getRecordContext();
        RepositoryManager repoMgr = indexUpdateBuilder.getRepositoryManager();

        // FIXME: it's more efficient to read all records at once
        // but make sure missing records are also treated (handled here via null linkedRecord in case of RecordNotFoundException
        if (ctx.record != null) {
            List links = IndexerUtils.flatList(ctx.record, fieldType);
            for (Link link: (List<Link>)links) {
                RecordId linkedRecordId = link.resolve(ctx.contextRecord, idGenerator);
                Record linkedRecord = null;
                String table = link.getTable() != null ? link.getTable() : indexUpdateBuilder.getTable();
                Repository repository = (Repository)repoMgr.getTable(table);
                try {
                    linkedRecord = VersionTag.getIdRecord(linkedRecordId, indexUpdateBuilder.getVTag(), repository);
                } catch (RecordNotFoundException rnfe) {
                    // ok, continue with null value
                } catch (VersionNotFoundException e) {
                    // ok, continue with null value
                }
                indexUpdateBuilder.push(linkedRecord, new Dep(linkedRecordId, Collections.<String>emptySet()));
                callback.call();
                indexUpdateBuilder.pop();
            }
        }
    }


}
