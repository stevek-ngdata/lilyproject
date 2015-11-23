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
package org.lilyproject.repository.impl.hbase;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.impl.hbase.LilyRecordVariantFilterProto.keyValue;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.util.ArgumentValidator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

/**
 * Actual implementation of an HBase filter to filter out variant lily records. This returns all records which have
 * exactly the given variant properties (i.e. not the records which have these variant properties and some additional
 * ones). If the value of the variant property is specified, it has to match exactly. If the value is
 * <code>null</code>, any value will match.
 */
public class LilyRecordVariantFilter extends FilterBase {
    private Map<String, String> variantProperties;
    private final IdGeneratorImpl idGenerator = new IdGeneratorImpl();

    /**
     * @param variantProperties the variant properties that the records should have
     */
    public LilyRecordVariantFilter(final Map<String, String> variantProperties) {
        ArgumentValidator.notNull(variantProperties, "variantProperties");

        this.variantProperties = variantProperties;
    }

    public LilyRecordVariantFilter() {
        // for hbase readFields
    }

    public Map<String, String> getVariantProperties() {
        return variantProperties;
    }

    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        // note: return value true means it is NOT a result of the scanner, false otherwise

        if (buffer == null) {
            return true;
        }

        final RecordId recordId = idGenerator.fromBytes(new DataInputImpl(buffer, offset, length));

        final SortedMap<String, String> recordVariantProperties = recordId.getVariantProperties();

        // check if the record has all expected variant properties
        if (containsAllExpectedDimensions(recordVariantProperties) &&
                hasSameValueForValuedDimensions(recordVariantProperties)) {

            // check if the record doesn't have other variant properties
            return variantProperties.size() != recordVariantProperties.size();
        } else {
            return true;
        }
    }

    private boolean containsAllExpectedDimensions(Map<String, String> recordVariantProperties) {
        return recordVariantProperties.keySet().containsAll(this.variantProperties.keySet());
    }

    private boolean hasSameValueForValuedDimensions(Map<String, String> variantProperties) {
        return variantProperties.entrySet().containsAll(getValuedDimensions().entrySet());
    }

    private Map<String, String> getValuedDimensions() {
        return Maps.filterValues(variantProperties, new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return input != null;
            }
        });
    }

    private LilyRecordVariantFilterProto.LilyRecordVariantFilter convert() {
        LilyRecordVariantFilterProto.LilyRecordVariantFilter.Builder builder = LilyRecordVariantFilterProto.LilyRecordVariantFilter.newBuilder();
        ArrayList<LilyRecordVariantFilterProto.keyValue> variantList = new ArrayList<LilyRecordVariantFilterProto.keyValue>();
        for (Map.Entry<String,String> variantProperty : variantProperties.entrySet()) {
            keyValue.Builder myKeyValue = keyValue.newBuilder();
            myKeyValue.setKey(variantProperty.getKey());
            myKeyValue.setValue(variantProperty.getValue() != null ? variantProperty.getValue() : "\u0000");
            variantList.add(myKeyValue.build());
        }
        builder.addAllVariantMap(variantList);
        return builder.build();
    }

    public byte[] toByteArray() { return convert().toByteArray(); }

    public static LilyRecordVariantFilter parseFrom(final byte[] pbBytes) throws DeserializationException {
        LilyRecordVariantFilterProto.LilyRecordVariantFilter proto;
        try {
            proto = LilyRecordVariantFilterProto.LilyRecordVariantFilter.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }

        ArrayList<LilyRecordVariantFilterProto.keyValue> variantList = new ArrayList<LilyRecordVariantFilterProto.keyValue>(proto.getVariantMapList());
        Map<String,String> variantMap = new HashMap<String, String>();
        for (LilyRecordVariantFilterProto.keyValue keyValue : variantList ) {
            final String key = keyValue.getKey();
            final String value = keyValue.getValue();
            variantMap.put(key,value.equals("\u0000") ? null : value);

        }

        return new LilyRecordVariantFilter(variantMap);
    }
/*
    public void write(DataOutput out) throws IOException {
        out.writeInt(variantProperties.size());
        for (Map.Entry<String, String> variantProperty : variantProperties.entrySet()) {
            out.writeUTF(variantProperty.getKey());
            out.writeUTF(variantProperty.getValue() != null ? variantProperty.getValue() : "\u0000");
        }
    }

    public void readFields(DataInput in) throws IOException {
        final int size = in.readInt();
        variantProperties = new HashMap<String, String>(size);
        for (int i = 0; i < size; i++) {
            final String key = in.readUTF();
            final String value = in.readUTF();
            variantProperties.put(key, value.equals("\u0000") ? null : value);
        }
    }
*/
}

