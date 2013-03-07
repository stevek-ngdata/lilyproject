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
package org.lilyproject.tools.recordrowvisualizer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import freemarker.template.TemplateException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.EncodingUtil;
import org.lilyproject.repository.impl.FieldFlags;
import org.lilyproject.repository.impl.HBaseTypeManager;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.repository.impl.id.SchemaIdImpl;
import org.lilyproject.util.Version;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.StateWatchingZooKeeper;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

/**
 * Tool to visualize the HBase-storage structure of a Lily record, in the form
 * of an HTML page.
 */
public class RecordRowVisualizer extends BaseZkCliTool {
    protected Option recordIdOption;
    protected Option tableOption;
    protected RecordRow recordRow;
    protected TypeManager typeMgr;
    protected ZooKeeperItf zk;

    @Override
    protected String getCmdName() {
        return "lily-record-row-visualizer";
    }

    @Override
    protected String getVersion() {
        return Version.readVersion("org.lilyproject", "lily-record-row-visualizer");
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        recordIdOption = OptionBuilder
                .withArgName("record-id")
                .hasArg()
                .withDescription("A Lily record ID: UUID.something or USER.something")
                .withLongOpt("record-id")
                .create("r");
        options.add(recordIdOption);

        tableOption = OptionBuilder
                .withArgName("table")
                .hasArg()
                .withDescription("Repository table name (defaults to record)")
                .withLongOpt("table")
                .create("t");
        options.add(tableOption);

        return options;
    }

    public static void main(String[] args) {
        new RecordRowVisualizer().start(args);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result =  super.run(cmd);
        if (result != 0)
            return result;

        String recordIdString = cmd.getOptionValue(recordIdOption.getOpt());
        if (recordIdString == null) {
            System.out.println("Specify record id with -" + recordIdOption.getOpt());
            return 1;
        }

        String tableName;
        if (cmd.hasOption(tableOption.getOpt())) {
            tableName = cmd.getOptionValue(tableOption.getOpt());
        } else {
            tableName = Table.RECORD.name;
        }

        IdGenerator idGenerator = new IdGeneratorImpl();
        RecordId recordId = idGenerator.fromString(recordIdString);

        recordRow = new RecordRow();
        recordRow.recordId = recordId;



        // HBase record table
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkConnectionString);
        HTableInterface table = new HTable(conf, tableName);

        // Type manager
        zk = new StateWatchingZooKeeper(zkConnectionString, zkSessionTimeout);
        typeMgr = new HBaseTypeManager(idGenerator, conf, zk, new HBaseTableFactoryImpl(conf));

        Get get = new Get(recordId.toBytes());
        get.setMaxVersions();
        Result row = table.get(get);

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long,byte[]>>> root = row.getMap();

        readColumns(root.get(RecordCf.DATA.bytes));

        byte[][] treatedColumnFamilies = {
                RecordCf.DATA.bytes
        };

        for (byte[] cf : root.keySet()) {
            if (!isInArray(cf, treatedColumnFamilies)) {
                recordRow.unknownColumnFamilies.add(Bytes.toString(cf));
            }
        }

        executeTemplate("recordrow2html.ftl",
                Collections.<String, Object>singletonMap("row", recordRow), System.out);

        return 0;
    }

    @Override
    protected void cleanup() {
        Closer.close(typeMgr);
        Closer.close(zk);
        HConnectionManager.deleteAllConnections(true);
        super.cleanup();
    }

    private boolean isInArray(byte[] key, byte[][] data) {
        for (byte[] item : data) {
            if (Arrays.equals(item, key))
                return true;
        }
        return false;
    }

    private void readColumns(NavigableMap<byte[], NavigableMap<Long, byte[]>> cf) throws Exception {
        Fields fields = recordRow.fields;

        for (Map.Entry<byte[], NavigableMap<Long, byte[]>> column : cf.entrySet()) {
            byte[] columnKey = column.getKey();

            if (columnKey[0] == RecordColumn.DATA_PREFIX) {
                SchemaId fieldId = new SchemaIdImpl(Arrays.copyOfRange(columnKey, 1, columnKey.length));

                for (Map.Entry<Long, byte[]> version : column.getValue().entrySet()) {
                    long versionNr = version.getKey();
                    byte[] value = version.getValue();

                    FieldType fieldType = fields.registerFieldType(fieldId, typeMgr);

                    Map<SchemaId, Object> columns = fields.values.get(versionNr);
                    if (columns == null) {
                        columns = new HashMap<SchemaId, Object>();
                        fields.values.put(versionNr, columns);
                    }

                    Object decodedValue;
                    if (FieldFlags.isDeletedField(value[0])) {
                        decodedValue = Fields.DELETED;
                    } else {
                        decodedValue = fieldType.getValueType().read(new DataInputImpl(EncodingUtil.stripPrefix(value)));
                    }

                    columns.put(fieldId, decodedValue);
                }
            } else if (Arrays.equals(columnKey, RecordColumn.DELETED.bytes)) {
                setSystemField("Deleted", column.getValue(), BOOLEAN_DECODER);
            } else if (Arrays.equals(columnKey, RecordColumn.NON_VERSIONED_RT_ID.bytes)) {
                setSystemField("Non-versioned Record Type ID", column.getValue(), new RecordTypeValueDecoder(typeMgr));
            } else if (Arrays.equals(columnKey, RecordColumn.NON_VERSIONED_RT_VERSION.bytes)) {
                setSystemField("Non-versioned Record Type Version", column.getValue(), LONG_DECODER);
            } else if (Arrays.equals(columnKey, RecordColumn.VERSIONED_RT_ID.bytes)) {
                setSystemField("Versioned Record Type ID", column.getValue(), new RecordTypeValueDecoder(typeMgr));
            } else if (Arrays.equals(columnKey, RecordColumn.VERSIONED_RT_VERSION.bytes)) {
                setSystemField("Versioned Record Type Version", column.getValue(), LONG_DECODER);
            } else if (Arrays.equals(columnKey, RecordColumn.VERSIONED_MUTABLE_RT_ID.bytes)) {
                setSystemField("Versioned-mutable Record Type ID", column.getValue(), new RecordTypeValueDecoder(typeMgr));
            } else if (Arrays.equals(columnKey, RecordColumn.VERSIONED_MUTABLE_RT_VERSION.bytes)) {
                setSystemField("Versioned-mutable Record Type Version", column.getValue(), LONG_DECODER);
            } else if (Arrays.equals(columnKey, RecordColumn.VERSION.bytes)) {
                setSystemField("Record Version", column.getValue(), LONG_DECODER);
            } else {
                recordRow.unknownColumns.add(Bytes.toString(columnKey));
            }
        }
    }

    private void setSystemField(String name, NavigableMap<Long, byte[]> valuesByVersion, ValueDecoder decoder) {
        SystemFields systemFields = recordRow.systemFields;
        SystemFields.SystemField systemField = systemFields.getOrCreateSystemField(name);
        for (Map.Entry<Long, byte[]> entry : valuesByVersion.entrySet()) {
            systemField.values.put(entry.getKey(), decoder.decode(entry.getValue()));
        }
    }

    public static interface ValueDecoder<T> {
        T decode(byte[] bytes);
    }

    public static class LongValueDecoder implements ValueDecoder<Long> {
        @Override
        public Long decode(byte[] bytes) {
            return Bytes.toLong(bytes);
        }
    }

    public static class BooleanValueDecoder implements ValueDecoder<Boolean> {
        @Override
        public Boolean decode(byte[] bytes) {
            return Bytes.toBoolean(bytes);
        }
    }

    public static class StringValueDecoder implements ValueDecoder<String> {
        @Override
        public String decode(byte[] bytes) {
            return Bytes.toString(bytes);
        }
    }

    public static class Base64ValueDecoder implements ValueDecoder<String> {
        @Override
        public String decode(byte[] bytes) {
            if (bytes == null)
                return null;

            char[] result = new char[bytes.length * 2];

            for (int i = 0; i < bytes.length; i++) {
                byte ch = bytes[i];
                result[2 * i] = Character.forDigit(Math.abs(ch >> 4), 16);
                result[2 * i + 1] = Character.forDigit(Math.abs(ch & 0x0f), 16);
            }

            return new String(result);
        }
    }

    public static class RecordTypeValueDecoder implements ValueDecoder<RecordTypeInfo> {
        private TypeManager typeManager;

        public RecordTypeValueDecoder(TypeManager typeManager) {
            this.typeManager = typeManager;
        }

        @Override
        public RecordTypeInfo decode(byte[] bytes) {
            SchemaId id = new SchemaIdImpl(bytes);
            QName name;
            try {
                name = typeManager.getRecordTypeById(id, null).getName();
            } catch (Exception e) {
                name = new QName("", "Failure retrieving record type name");
            }
            return new RecordTypeInfo(id, name);
        }
    }

    private static final StringValueDecoder STRING_DECODER = new StringValueDecoder();
    private static final BooleanValueDecoder BOOLEAN_DECODER = new BooleanValueDecoder();
    private static final LongValueDecoder LONG_DECODER = new LongValueDecoder();
    private static final Base64ValueDecoder BASE64_DECODER = new Base64ValueDecoder();

    private void executeTemplate(String template, Map<String, Object> variables, OutputStream os) throws IOException, TemplateException {
        new TemplateRenderer().render(template, variables, os);

    }
}