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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.IdRecordScanner;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.api.VersionNotFoundException;
import org.lilyproject.repository.api.filter.RecordVariantFilter;
import org.lilyproject.util.repo.VersionTag;

public class IndexerUtils {

    private IndexerUtils() {
    }

    public static ArrayList<Record> getVariantsAsRecords(IndexUpdateBuilder indexUpdateBuilder, Dep newDep) throws IOException, RepositoryException, InterruptedException {

        // build a variant properties map which is a combination of dep.id.variantProperties + dep.moreDimensionedVariants
        final Map<String, String> varProps = new HashMap<String, String>(newDep.id.getVariantProperties());
        varProps.putAll(newDep.id.getVariantProperties());
        for (String vprop: newDep.moreDimensionedVariants) {
            varProps.put(vprop, null);
        }

        final ArrayList<Record> result = new ArrayList<Record>();

        final RecordScan scan = new RecordScan();
        scan.setRecordFilter(new RecordVariantFilter(newDep.id.getMaster(), varProps));
        LRepository repository = indexUpdateBuilder.getRepository();
        LTable table = repository.getTable(indexUpdateBuilder.getTable());
        final IdRecordScanner scanner = table.getScannerWithIds(scan);
        IdRecord next;
        while ((next = scanner.next()) != null) {
            try {
                final Record record = VersionTag.getIdRecord(next, indexUpdateBuilder.getVTag(), table, repository);
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

    /**
     * Extracts the given field value from the record and transforms it into a flat (java) list,
     * regardless of the valueType of the field.
     * @param record
     * @param fieldType
     * @return
     */
    public static List flatList(Record record, FieldType fieldType) {
        if (record != null && record.hasField(fieldType.getName())) {
            return flatList(record.getField(fieldType.getName()), fieldType.getValueType());
        } else {
            return Collections.emptyList();
        }
    }

    public static List flatList(Object value, ValueType type) {
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


}
