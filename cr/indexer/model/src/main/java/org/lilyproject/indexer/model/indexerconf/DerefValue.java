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
    private List<Follow> crossRecordFollows = new ArrayList<Follow>();
    private FieldType targetField;
    private FieldType lastRealField;

    protected DerefValue(FieldType targetField, boolean extractContent, String formatter) {
        super(extractContent, formatter);
        this.targetField = targetField;
    }

    /**
     * This method should be called after all follow-expressions have been added.
     */
    protected void init(TypeManager typeManager) throws RepositoryException, InterruptedException {
        follows = Collections.unmodifiableList(follows);
        crossRecordFollows = Collections.unmodifiableList(crossRecordFollows);

        // To find the lastRealField:
        //   - run over the follows in inverse order
        //   - on encountering the first Follow which is not a RecordFieldFollow, take the field of the
        //     RecordFieldFollow which follows it, or the target field if we're at the last entry.
        //   - it is possible that we reach the end and only encountered RecordFieldFollows, in that
        //     case the lastRealField stays null

        for (int i = follows.size() - 1; i >= 0; i--) {
            if (follows.get(i) instanceof RecordFieldFollow) {
                continue;
            }

            if (i == follows.size() - 1) {
                lastRealField = targetField;
            } else {
                lastRealField = ((RecordFieldFollow)follows.get(i + 1)).getFieldType();
            }
            break;
        }

        //
        // Set the ownerFieldType property for LinkFieldFollow's
        //
        RecordFieldFollow currentRootRecordFieldFollow = null;
        for (Follow follow : follows) {
            if (follow instanceof LinkFieldFollow) {
                if (currentRootRecordFieldFollow != null) {
                    ((LinkFieldFollow)follow).ownerFieldType = currentRootRecordFieldFollow.getFieldType();
                }
                currentRootRecordFieldFollow = null;
            } else if (follow instanceof MasterFollow || follow instanceof VariantFollow) {
                if (currentRootRecordFieldFollow != null) {
                    throw new RuntimeException("Unexpected situation: master or variant follow after record" +
                            " follow: this should have been validated by the indexer conf parser.");
                }
                currentRootRecordFieldFollow = null;
            } else if (follow instanceof RecordFieldFollow) {
                if (currentRootRecordFieldFollow == null) {
                    currentRootRecordFieldFollow = (RecordFieldFollow)follow;
                }
            } else {
                throw new RuntimeException("Unexpected follow impl: " + follow.getClass().getName());
            }
        }
    }

    protected void addLinkFieldFollow(FieldType fieldType) {
        LinkFieldFollow follow = new LinkFieldFollow(fieldType);
        follows.add(follow);
        crossRecordFollows.add(follow);
    }

    protected void addRecordFieldFollow(FieldType fieldType) {
        follows.add(new RecordFieldFollow(fieldType));
    }

    protected void addMasterFollow() {
        MasterFollow follow = new MasterFollow();
        follows.add(follow);
        crossRecordFollows.add(follow);
    }

    protected void addVariantFollow(Set<String> dimensions) {
        VariantFollow follow = new VariantFollow(dimensions);
        follows.add(follow);
        crossRecordFollows.add(follow);
    }

    public List<Follow> getFollows() {
        return follows;
    }

    public List<Follow> getCrossRecordFollows() {
        return crossRecordFollows;
    }

    /**
     * Returns the field taken from the document to which the follow-expressions point, thus the last
     * field in the chain.
     */
    public FieldType getTargetField() {
        return targetField;
    }

    /**
     * Returns the last field in the dereference chain which is not a field from an embedded record.
     * This can be null, when the dereferencing only goes through RECORD fields.
     */
    public FieldType getLastRealField() {
        return lastRealField;
    }

    public static interface Follow {
    }

    public static class LinkFieldFollow implements Follow {
        FieldType fieldType;
        /**
         * If the link field follow is after one or more record follows, then from the point of view
         * of the link index, the link belongs to the same field as the top-level record field. We
         * keep a reference to that field here.
         */
        FieldType ownerFieldType;

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
    }

    public static class RecordFieldFollow implements Follow {
        FieldType fieldType;

        public RecordFieldFollow(FieldType fieldType) {
            this.fieldType = fieldType;
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

    @Override
    public SchemaId getFieldDependency() {
        if (follows.get(0) instanceof LinkFieldFollow) {
            return ((LinkFieldFollow)follows.get(0)).fieldType.getId();
        } else {
            // A follow-variant is like a link to another document, but the link can never change as the
            // identity of the document never changes. Therefore, there is no dependency on a field.
            return null;
        }
    }

    @Override
    public FieldType getTargetFieldType() {
        return targetField;
    }
}
