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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.IdRecordScanner;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.api.VersionNotFoundException;
import org.lilyproject.repository.api.filter.RecordVariantFilter;
import org.lilyproject.util.repo.SystemFields;
import org.lilyproject.util.repo.VTaggedRecord;
import org.lilyproject.util.repo.VersionTag;

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
    public void collectIndexUpdate(IndexUpdateBuilder indexUpdateBuilder)
            throws InterruptedException, RepositoryException {
        RecordContext ctx = indexUpdateBuilder.getRecordContext();
        if (fieldType != null && !systemFields.isSystemField(fieldType.getName())) {
            if (!ctx.record.hasField(fieldType.getName())) {
                return;
            }
        }

        Repository repository = indexUpdateBuilder.getRepository();
        IdGenerator idGenerator = repository.getIdGenerator();

        if (follow instanceof LinkFieldFollow) {
            FieldType fieldType = ((LinkFieldFollow)follow).getFieldType();
            if (!systemFields.isSystemField(fieldType.getName())) {
                indexUpdateBuilder.addDependency(fieldType.getId());
            }
            if (ctx.record != null) {
                List links = flatList(ctx.record, fieldType);
                for (Link link: (List<Link>)links) {
                    RecordId linkedRecordId = link.resolve(ctx.contextRecord, idGenerator);
                    Record linkedRecord = null;
                    try {
                        linkedRecord = VersionTag.getIdRecord(linkedRecordId, indexUpdateBuilder.getVTag(), repository);
                    } catch (RecordNotFoundException rnfe) {
                        // ok
                    } catch (VersionNotFoundException vnfe) {
                        // ok
                    }
                    indexUpdateBuilder.push(linkedRecord, new Dep(linkedRecordId, Collections.<String>emptySet()));
                    super.collectIndexUpdate(indexUpdateBuilder);
                    indexUpdateBuilder.pop();
                }
            }
        } else if (follow instanceof RecordFieldFollow) {
            FieldType fieldType = ((RecordFieldFollow)follow).getFieldType();
            if (!systemFields.isSystemField(fieldType.getName())) {
                indexUpdateBuilder.addDependency(fieldType.getId());
            }
            if (ctx.record != null) {
                List records = flatList(ctx.record, fieldType);
                for (Record record: (List<Record>)records) {
                    indexUpdateBuilder.push(record, ctx.contextRecord, ctx.dep); // TODO: pass null instead of ctx.dep?
                    super.collectIndexUpdate(indexUpdateBuilder);
                    indexUpdateBuilder.pop();
                }
            }
        } else if (follow instanceof MasterFollow) {
            Dep masterDep = new Dep(ctx.dep.id.getMaster(), Collections.<String>emptySet());
            Record record = null;
            try {
                record = VersionTag.getIdRecord(masterDep.id, indexUpdateBuilder.getVTag(), repository);
            } catch (RecordNotFoundException rnfe) {
                // OK, continue with 'null' record
            } catch (VersionNotFoundException vnfe) {
                // OK, continue with 'null' record
            }
            indexUpdateBuilder.push(record, masterDep);
            super.collectIndexUpdate(indexUpdateBuilder);
            indexUpdateBuilder.pop();
        } else if (follow instanceof VariantFollow) {
            Dep dep = ctx.dep.minus(idGenerator, ((VariantFollow)follow).getDimensions());
            Record record = null;
            try {
                record = VersionTag.getIdRecord(dep.id, indexUpdateBuilder.getVTag(), repository);
            } catch (RecordNotFoundException rnfe) {
                // OK, continue with 'null' record
            } catch (VersionNotFoundException vnfe) {
                // OK, continue with 'null' record
            }
            indexUpdateBuilder.push(record, dep);
            super.collectIndexUpdate(indexUpdateBuilder);
            indexUpdateBuilder.pop();
        } else if (follow instanceof ForwardVariantFollow) {
            Map<String, String> dimensions = ((ForwardVariantFollow)follow).getDimensions();
            Set<String> currentDimensions = Sets.newHashSet(ctx.dep.vprops);
            currentDimensions.addAll(ctx.dep.id.getVariantProperties().keySet());
            if (currentDimensions.containsAll(dimensions.keySet())) {
                // we already have all of the specified dimensions => stop here
            } else {
                Dep dep = ctx.dep.plus(idGenerator, dimensions);
                handleForwardVariantFollow(indexUpdateBuilder, dep);
            }
        }
    }

    private List flatList(Record record, FieldType fieldType) {
        if (record != null && record.hasField(fieldType.getName())) {
            return flatList(record.getField(fieldType.getName()), fieldType.getValueType());
        } else {
            return Collections.emptyList();
        }
    }

    private List flatList(Object value, ValueType type) {
        if (type.getBaseName().equals("LIST")) {
            if (type.getNestedValueType().getBaseName() != "LIST") {
                return (List)value;
            } else {
                List result = Lists.newArrayList();
                for (Object nValue: (List)value) {
                    result.addAll(flatList(nValue, type.getNestedValueType()));
                }
                return result;
            }
        } else {
            return Collections.singletonList(value);
        }
    }

    private void handleForwardVariantFollow(IndexUpdateBuilder indexUpdateBuilder, Dep dep) throws RepositoryException,
            InterruptedException {

        List<Record> records = scanVariants(indexUpdateBuilder, dep);
        if (records == null || records.size() == 0) {
            // variant/master does not exist - continue with 'null' record to make sure the dependencies are added
            indexUpdateBuilder.push(null, dep);
            super.collectIndexUpdate(indexUpdateBuilder);
            indexUpdateBuilder.pop();
        } else {
            for (Record record: records) {
                indexUpdateBuilder.push(record, dep);
                super.collectIndexUpdate(indexUpdateBuilder);
                indexUpdateBuilder.pop();
            }
        }
    }

    // FIXME: duplicated code - see ValueEvaluator
    public ArrayList<Record> scanVariants(IndexUpdateBuilder indexUpdateBuilder, Dep newDep) throws RepositoryException, InterruptedException {

        // build a variant properties map which is a combination of dep.id.variantProperties + dep.vprops
        final Map<String, String> varProps = new HashMap<String, String>(newDep.id.getVariantProperties());
        varProps.putAll(newDep.id.getVariantProperties());
        for (String vprop: newDep.vprops) {
            varProps.put(vprop, null);
        }

        final ArrayList<Record> result = new ArrayList<Record>();

        final RecordScan scan = new RecordScan();
        scan.setRecordFilter(new RecordVariantFilter(newDep.id.getMaster(), varProps));
        Repository repository = indexUpdateBuilder.getRepository();
        final IdRecordScanner scanner = repository.getScannerWithIds(scan);
        IdRecord next;
        while ((next = scanner.next()) != null) {
            try {
                final Record record = VersionTag.getIdRecord(next, indexUpdateBuilder.getVTag(), indexUpdateBuilder.getRepository());
                result.add(record);
            } catch (RecordNotFoundException rnfe) {
                //ok
            } catch (VersionNotFoundException vnfe) {
                //ok
            }
        }

        scanner.close();
        return result;
    }


}
