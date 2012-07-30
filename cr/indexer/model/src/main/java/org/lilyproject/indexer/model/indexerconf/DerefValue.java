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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeManager;

public class DerefValue extends BaseValue {
    private List<Follow> follows = new ArrayList<Follow>();
    private List<Follow> crossRecordFollows = new ArrayList<Follow>();
    private Value value;
    private FieldType targetField;
    private FieldType lastRealField;

    /**
     * NOTE: targetField is the 'artificial' target field that takes the single/multivaluedness of the
     * follows into account
     */
    protected DerefValue(List<Follow> follows, Value value, FieldType targetField, boolean extractContent, String formatter) {
        super(extractContent, formatter);
        //NOTE: extractContent and formatter should be identical to the one for value
        // so it doesn't matter which ones are used by the ValueEvaluator

        this.follows = follows;
        this.value = value;
        this.targetField = targetField;
    }

    /**
     * This method should be called after all follow-expressions have been added.
     */
    protected void init(TypeManager typeManager) throws RepositoryException, InterruptedException {
        follows = Collections.unmodifiableList(follows);

        for (Follow follow: follows) {
            if (follow instanceof MasterFollow
                  || follow instanceof VariantFollow
                  || follow instanceof ForwardVariantFollow
                  || follow instanceof LinkFieldFollow) {
                crossRecordFollows.add(follow);
            }
        }
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
                lastRealField = ((RecordFieldFollow) follows.get(i + 1)).getFieldType();
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
                    ((LinkFieldFollow) follow).setOwnerFieldType(currentRootRecordFieldFollow.getFieldType());
                }
                currentRootRecordFieldFollow = null;
            } else if (follow instanceof MasterFollow || follow instanceof VariantFollow ||
                    follow instanceof ForwardVariantFollow) {
                if (currentRootRecordFieldFollow != null) {
                    throw new RuntimeException("Unexpected situation: master or variant follow after record" +
                            " follow: this should have been validated by the indexer conf parser.");
                }
                currentRootRecordFieldFollow = null;
            } else if (follow instanceof RecordFieldFollow) {
                if (currentRootRecordFieldFollow == null) {
                    currentRootRecordFieldFollow = (RecordFieldFollow) follow;
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

    protected void addForwardVariantFollow(Map<String, String> dimensions) {
        ForwardVariantFollow follow = new ForwardVariantFollow(dimensions);
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

    @Override
    public SchemaId getFieldDependency() {
        if (follows.get(0) instanceof LinkFieldFollow) {
            return ((LinkFieldFollow) follows.get(0)).getFieldType().getId();
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
