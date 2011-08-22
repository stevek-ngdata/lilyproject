package org.lilyproject.tools.recordrowvisualizer;

import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.EncodingUtil;
import org.lilyproject.repository.impl.SchemaIdImpl;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;

import java.util.*;

public class Fields {
    Map<Long, Map<SchemaId, Object>> values = new HashMap<Long, Map<SchemaId, Object>>();
    List<Type<FieldType>> fields = new ArrayList<Type<FieldType>>();
    long minVersion = Integer.MAX_VALUE;
    long maxVersion = Integer.MIN_VALUE;

    Object DELETED = new Object();

    public Fields(NavigableMap<byte[], NavigableMap<Long,byte[]>> cf, TypeManager typeMgr, Repository repository, Scope scope) throws Exception {
        // The fields are rotated so that the version is the primary point of access.
        // Also, field values are decoded, etc. 
        for (Map.Entry<byte[], NavigableMap<Long,byte[]>> entry : cf.entrySet()) {
            byte[] column = entry.getKey();
            if (column[0] == RecordColumn.DATA_PREFIX) {
                SchemaId fieldId = new SchemaIdImpl(Arrays.copyOfRange(column, 1, column.length));
    
                for (Map.Entry<Long, byte[]> columnEntry : entry.getValue().entrySet()) {
                    long version = columnEntry.getKey();
                    byte[] value = columnEntry.getValue();
    
                    FieldType fieldType = registerFieldType(fieldId, typeMgr, scope);
    
                    if (fieldType != null) {
                        Map<SchemaId, Object> columns = values.get(version);
                        if (columns == null) {
                            columns = new HashMap<SchemaId, Object>();
                            values.put(version, columns);
                        }
    
                        Object decodedValue;
                        if (EncodingUtil.isDeletedField(value)) {
                            decodedValue = DELETED;
                        } else {
                            decodedValue = fieldType.getValueType().read(new DataInputImpl(EncodingUtil.stripPrefix(value)), repository);
                        }
        
                        columns.put(fieldId, decodedValue);
        
                        if (version > maxVersion) {
                            maxVersion = version;
                        }
        
                        if (version < minVersion) {
                            minVersion = version;
                        }
                    }
                }
            }
        }
    }

    private FieldType registerFieldType(SchemaId fieldId, TypeManager typeMgr, Scope scope) throws Exception {
        for (Type<FieldType> entry : fields) {
            if (entry.id.equals(fieldId))
                return entry.object;
        }

        Type<FieldType> type = new Type<FieldType>();
        type.id = fieldId;
        type.object = typeMgr.getFieldTypeById(fieldId);
        
        // Filter the scope
        if (scope.equals(type.object.getScope())) {
            fields.add(type);
            return type.object;
        }
        return null;
    }

    public List<Type<FieldType>> getFieldTypes() {
        return fields;
    }

    public Object getValue(long version, SchemaId fieldId) {
        Map<SchemaId, Object> valuesByColumn = values.get(version);
        if (valuesByColumn == null)
            return null;
        return valuesByColumn.get(fieldId);
    }

    public boolean isNull(long version, SchemaId fieldId) {
        Map<SchemaId, Object> valuesByColumn = values.get(version);
        return valuesByColumn == null || !valuesByColumn.containsKey(fieldId);
    }

    public boolean isDeleted(long version, SchemaId fieldId) {
        Map<SchemaId, Object> valuesByColumn = values.get(version);
        return valuesByColumn != null && valuesByColumn.get(fieldId) == DELETED;
    }

    public long getMinVersion() {
        return minVersion;
    }

    public long getMaxVersion() {
        return maxVersion;
    }
}
