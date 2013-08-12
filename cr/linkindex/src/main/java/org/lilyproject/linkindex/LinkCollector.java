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
package org.lilyproject.linkindex;

import java.util.HashSet;
import java.util.Set;

import org.lilyproject.repository.api.AbsoluteRecordId;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.lilyproject.util.hbase.RepoAndTableUtil;

public class LinkCollector {
    private IdGenerator idGenerator;
    private Set<FieldedLink> links = new HashSet<FieldedLink>();

    public LinkCollector(IdGenerator idGenerator) {
        this.idGenerator = idGenerator;
    }

    public void addLink(RecordId target, SchemaId fieldTypeId) {
        //FIXME: multi repository
        addLink(idGenerator.newAbsoluteRecordId(RepoAndTableUtil.DEFAULT_REPOSITORY ,Table.RECORD.name, target), fieldTypeId);
    }

    public void addLink(AbsoluteRecordId target, SchemaId fieldTypeId) {
        links.add(new FieldedLink(target, fieldTypeId));
    }

    public Set<FieldedLink> getLinks() {
        return links;
    }
}
