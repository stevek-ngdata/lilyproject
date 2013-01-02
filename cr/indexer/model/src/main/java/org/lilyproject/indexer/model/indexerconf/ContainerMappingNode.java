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

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.util.repo.VTaggedRecord;

public abstract class ContainerMappingNode implements MappingNode {

    private final List<MappingNode> children = Lists.newArrayList();

    public void addChildNode(MappingNode node) {
        children.add(node);
    }

    public List<MappingNode> getChildren() {
        return children;
    }

    public boolean childrenAffectedByUpdate(VTaggedRecord record, Scope scope) throws InterruptedException, RepositoryException {
        for (MappingNode child : getChildren()) {
            if (child.isIndexAffectedByUpdate(record, scope)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void collectIndexUpdate(IndexUpdateBuilder indexUpdateBuilder) throws InterruptedException, RepositoryException {
        for (MappingNode child : getChildren()) {
            child.collectIndexUpdate(indexUpdateBuilder);
        }
    }

    public void visitAll(Predicate<MappingNode> predicate) {
        if (predicate.apply(this)) {
            for (MappingNode child : children) {
                child.visitAll(predicate);
            }
        }
    }

}
