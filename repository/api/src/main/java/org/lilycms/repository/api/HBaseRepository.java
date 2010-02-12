package org.lilycms.repository.api;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseRepository implements Repository {

    private static final byte[] DEFAULT_COLUMN_FAMILY = Bytes.toBytes("defaultCF");
    private static final String RECORD_TABLE = "recordTable";
    private HTable recordTable;

    public HBaseRepository(Configuration configuration) throws IOException {
        try {
            recordTable = new HTable(configuration, RECORD_TABLE);
        } catch (IOException e) {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            HTableDescriptor tableDescriptor = new HTableDescriptor(RECORD_TABLE);
            tableDescriptor.addFamily(new HColumnDescriptor(DEFAULT_COLUMN_FAMILY));
            admin.createTable(tableDescriptor);
            recordTable = new HTable(configuration, RECORD_TABLE);
        }
    }

    public void create(Record record) throws IOException, RecordExistsException {
        String recordId = record.getRecordId();
        Get get = new Get(Bytes.toBytes(recordId));
        Result result = recordTable.get(get);
        if (!result.isEmpty()) {
            throw new RecordExistsException(recordId);
        }
        Put put = createPut(record);
        recordTable.put(put);
    }

    public Record read(String recordId) throws IOException {
        return read(recordId, null);
    }

    public Record read(String recordId, Set<String> fieldNames) throws IOException {
        Get get = new Get(Bytes.toBytes(recordId));
        if (fieldNames != null) {
            for (String fieldName : fieldNames) {
                get.addColumn(DEFAULT_COLUMN_FAMILY, Bytes.toBytes(fieldName));
            }
        }
        Result result = recordTable.get(get);
        return createRecord(result);
    }

    public void update(Record record) throws IOException, NoSuchRecordException {
        Get get = new Get(Bytes.toBytes(record.getRecordId()));
        Result result = recordTable.get(get);
        if (result.isEmpty()) {
            throw new NoSuchRecordException(record.getRecordId());
        }

        Put put = createPut(record);
        recordTable.put(put);
    }

    public void delete(String recordId) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(recordId));
        recordTable.delete(delete);
        
    }

    private Put createPut(Record record) throws IOException {
        Put put = new Put(Bytes.toBytes(record.getRecordId()));
        Set<Field> fields = record.getFields();
        if (fields.isEmpty()) {
            // TODO throw decent exception
            // TODO is it actually a problem in case of updates?
            throw new IOException("Record <" + record.getRecordId() + "> does not contain any fields");
        }
        for (Field field : fields) {
            put.add(DEFAULT_COLUMN_FAMILY, Bytes.toBytes(field.getName()), field.getValue());
        }
        return put;
    }

    private Record createRecord(Result result) {
        Record record;
        byte[] rowKey = result.getRow();
        if (rowKey == null)
            return null;
        record = new Record(new String(rowKey));
        NavigableMap<byte[], byte[]> defaultFamilyMap = result.getFamilyMap(DEFAULT_COLUMN_FAMILY);
        Set<Entry<byte[], byte[]>> entrySet = defaultFamilyMap.entrySet();
        for (Entry<byte[], byte[]> entry : entrySet) {
            record.addField(new Field(new String(entry.getKey()), entry.getValue()));
        }
        return record;
    }

}
