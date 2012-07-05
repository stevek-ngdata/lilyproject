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


import com.google.common.base.Predicate;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.util.repo.VTaggedRecord;

public class IndexField implements MappingNode {

    private final String name;
    private final Value value;

    public IndexField(String name, Value value) {
        this.name = name;
        this.value = value;
    }

    public Value getValue() {
        return value;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean isIndexAffectedByUpdate(VTaggedRecord vtRecord, Scope scope) throws InterruptedException,
            RepositoryException {
        return vtRecord.getUpdatedFieldTypeIdsByScope().get(scope).contains(value.getFieldDependency());
    }

    @Override
    public void visitAll(Predicate<MappingNode> predicate) {
        predicate.apply(this);
    }

    @Override
    public void collectIndexUpdate(IndexUpdateBuilder indexUpdateBuilder, Record record, long version, SchemaId vtag) throws InterruptedException, RepositoryException {
        indexUpdateBuilder.addField(name, indexUpdateBuilder.eval(value));
    }

}
