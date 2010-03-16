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
package org.lilycms.repository.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.api.RecordType;

public class EncodingUtil {
    
    public static final byte EXISTS_FLAG = (byte) 0;
    public static final byte DELETE_FLAG = (byte) 1;

    public static boolean isDeletedField(byte[] value) {
        return value[0] == DELETE_FLAG;
    }

    public static byte[] prefixValue(byte[] fieldValue, byte prefix) {
        byte[] prefixedValue;
        prefixedValue = new byte[fieldValue.length + 1];
        prefixedValue[0] = prefix;
        System.arraycopy(fieldValue, 0, prefixedValue, 1, fieldValue.length);
        return prefixedValue;
    }

    public static byte[] stripPrefix(byte[] prefixedValue) {
        return Arrays.copyOfRange(prefixedValue, 1, prefixedValue.length);
    }
    
    //TODO make rowkey encodings robust (backward compatible) for different encoding versions
    //For example : make the rowkeys namespace aware
    /**
     * Generates an new HBase rowkey based on the recordId and optional variantProperties.
     */
    public static byte[] generateRecordRowKey(RecordId recordId, Map<String, String> variantProperties) {
        StringBuffer rowKey = new StringBuffer();
        rowKey.append(recordId);
        if (variantProperties != null) {
            ArrayList<String> dimensions = new ArrayList<String>(variantProperties.keySet());
            Collections.sort(dimensions);
            for (String dimension : dimensions) {
                rowKey.append('|');
                rowKey.append(dimension);
                rowKey.append('|');
                rowKey.append(variantProperties.get(dimension));
            }
        }
        return Bytes.toBytes(rowKey.toString());
    }
    
    /**
     * Generates a new HBase rowkey based on the recordTypeId.
     */
    public static byte[] generateRecordTypeRowKey(String recordTypeId) {
        return Bytes.toBytes(recordTypeId);
    }
    
    /**
     * Generates a new HBase rowkey based on the recordTypeId and fieldDescriptorId.
     */
    public static byte[] generateFieldDescriptorRowKey(String recordTypeId, String fieldDescriptorId) {
        StringBuffer rowKey = new StringBuffer();
        rowKey.append(recordTypeId);
        rowKey.append('|');
        rowKey.append(fieldDescriptorId);
        return Bytes.toBytes(rowKey.toString());
    }
}
