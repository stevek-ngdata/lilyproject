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
package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.*;

import java.util.*;

public class DerefValue extends BaseValue {
    private List<Follow> follows = new ArrayList<Follow>();
    private FieldType fieldType;

    protected DerefValue(FieldType fieldType, boolean extractContent, String formatter) {
        super(extractContent, formatter);
        this.fieldType = fieldType;
    }

    /**
     * This method should be called after all follow-expressions have been added.
     */
    protected void init(TypeManager typeManager) throws RepositoryException, InterruptedException {
        follows = Collections.unmodifiableList(follows);
    }

    protected void addFieldFollow(FieldType fieldType) {
        follows.add(new FieldFollow(fieldType));
    }

    protected void addMasterFollow() {
        follows.add(new MasterFollow());
    }

    protected void addVariantFollow(Set<String> dimensions) {
        follows.add(new VariantFollow(dimensions));
    }

    public List<Follow> getFollows() {
        return follows;
    }

    /**
     * Returns the field taken from the document to which the follow-expressions point, thus the last
     * field in the chain.
     */
    public FieldType getTargetField() {
        return fieldType;
    }

    public static interface Follow {
    }

    public static class FieldFollow implements Follow {
        FieldType fieldType;

        public FieldFollow(FieldType fieldType) {
            this.fieldType = fieldType;
        }

        public SchemaId getFieldId() {
            return fieldType.getId();
        }

        public FieldType getFieldType() {
            return fieldType;
        }
    }

    public static class MasterFollow implements Follow {
    }

    public static class VariantFollow implements Follow {
        private Set<String> dimensions;

        public VariantFollow(Set<String> dimensions) {
            this.dimensions = dimensions;
        }

        public Set<String> getDimensions() {
            return dimensions;
        }
    }

    public SchemaId getFieldDependency() {
        if (follows.get(0) instanceof FieldFollow) {
            return ((FieldFollow)follows.get(0)).fieldType.getId();
        } else {
            // A follow-variant is like a link to another document, but the link can never change as the
            // identity of the document never changes. Therefore, there is no dependency on a field.
            return null;
        }
    }

    public FieldType getTargetFieldType() {
        return fieldType;
    }
}
