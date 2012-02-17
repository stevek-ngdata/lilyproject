package org.lilyproject.tools.recordrowvisualizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.kauriproject.template.*;
import org.kauriproject.template.source.ClasspathSourceResolver;
import org.kauriproject.template.source.Source;
import org.kauriproject.template.source.SourceResolver;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.EncodingUtil;
import org.lilyproject.repository.impl.HBaseTypeManager;
import org.lilyproject.repository.impl.recordid.IdGeneratorImpl;
import org.lilyproject.repository.impl.SchemaIdImpl;
import org.lilyproject.rowlog.api.ExecutionState;
import org.lilyproject.rowlog.impl.SubscriptionExecutionState;
import org.lilyproject.util.hbase.HBaseAdminFactory;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.StateWatchingZooKeeper;
import org.lilyproject.util.zookeeper.ZooKeeperItf;
import org.xml.sax.SAXException;

import static org.lilyproject.util.hbase.LilyHBaseSchema.*;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * Tool to visualize the HBase-storage structure of a Lily record, in the form
 * of an HTML page.
 */
public class RecordRowVisualizer extends BaseZkCliTool {
    protected Option recordIdOption;
    protected RecordRow recordRow;
    protected TypeManager typeMgr;
    protected ZooKeeperItf zk;

    @Override
    protected String getCmdName() {
        return "lily-record-row-visualizer";
    }

    @Override
    protected String getVersion() {
        return readVersion("org.lilyproject", "lily-record-row-visualizer");
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

        IdGenerator idGenerator = new IdGeneratorImpl();
        RecordId recordId = idGenerator.fromString(recordIdString);

        recordRow = new RecordRow();
        recordRow.recordId = recordId;


        // HBase record table
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkConnectionString);
        HTableInterface table = new HTable(conf, Table.RECORD.bytes);

        // Type manager
        zk = new StateWatchingZooKeeper(zkConnectionString, zkSessionTimeout);
        typeMgr = new HBaseTypeManager(idGenerator, conf, zk, new HBaseTableFactoryImpl(conf));

        Get get = new Get(recordId.toBytes());
        get.setMaxVersions();
        Result row = table.get(get);

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long,byte[]>>> root = row.getMap();

        readColumns(root.get(RecordCf.DATA.bytes));

        readRowLog(recordRow.walState, recordRow.walPayload, recordRow.mqState, recordRow.mqPayload,
                root.get(RecordCf.ROWLOG.bytes));

        byte[][] treatedColumnFamilies = {
                RecordCf.DATA.bytes,
                RecordCf.ROWLOG.bytes
        };

        for (byte[] cf : root.keySet()) {
            if (!isInArray(cf, treatedColumnFamilies)) {
                recordRow.unknownColumnFamilies.add(Bytes.toString(cf));
            }
        }

        executeTemplate("org/lilyproject/tools/recordrowvisualizer/recordrow2html.xml",
                Collections.<String, Object>singletonMap("row", recordRow), System.out);

        return 0;
    }

    @Override
    protected void cleanup() {
        Closer.close(typeMgr);
        Closer.close(zk);
        HConnectionManager.deleteAllConnections(true);
        HBaseAdminFactory.closeAll();
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
                    if (EncodingUtil.isDeletedField(value)) {
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
            } else if (Arrays.equals(columnKey, RecordColumn.LOCK.bytes)) {
                setSystemField("Lock", column.getValue(), BASE64_DECODER);
            } else if (Arrays.equals(columnKey, RecordColumn.VERSION.bytes)) {
                setSystemField("Record Version", column.getValue(), LONG_DECODER);
            } else {
                recordRow.unknownColumns.add(Bytes.toString(columnKey));
            }
        }
    }

    private void readRowLog(Map<RowLogKey, List<ExecutionData>> walStateByKey, Map<RowLogKey,
            List<String>> walPayloadByKey, Map<RowLogKey, List<ExecutionData>> mqStateByKey,
            Map<RowLogKey, List<String>> mqPayloadByKey, NavigableMap<byte[], NavigableMap<Long, byte[]>> cf)
            throws IOException {

        if (cf == null)
            return;
        
        for (Map.Entry<byte[], NavigableMap<Long, byte[]>> rowEntry : cf.entrySet()) {
            byte[] column = rowEntry.getKey();
            // columns start with rowlow-prefix
            byte rowlogId = column[0];
            if (rowlogId == RecordColumn.WAL_PREFIX) {
                readRowLog(walStateByKey, walPayloadByKey, rowEntry.getValue(), Arrays.copyOfRange(column, 1, column.length));
            } else if (rowlogId == RecordColumn.MQ_PREFIX) {
                readRowLog(mqStateByKey, mqPayloadByKey, rowEntry.getValue(), Arrays.copyOfRange(column, 1, column.length));
            } else {
                // TODO : unknown rowlog
            }
        }
    }
    
    private static final byte PL_BYTE = (byte)1;
    private static final byte ES_BYTE = (byte)2;
    private static final byte[] SEQ_NR = Bytes.toBytes("SEQNR");
    
    private void readRowLog(Map<RowLogKey, List<ExecutionData>> stateByKey, Map<RowLogKey,
            List<String>> payloadByKey, NavigableMap<Long, byte[]> columnCells, byte[] key) throws IOException {

        NavigableMap<Long, byte[]> maxSeqNr = null;
        
        if (Arrays.equals(key, SEQ_NR)) { // key could be "SEQNR"
            maxSeqNr = columnCells;
        } else { // or is prefixed by payload or executionState byte
            long seqNr = Bytes.toLong(key, 1);
            long timestamp = Bytes.toLong(key, 1 + Bytes.SIZEOF_LONG);
            if (key[0] == PL_BYTE) {
                readPayload(payloadByKey, columnCells, seqNr, timestamp);
            } else if (key[0] == ES_BYTE) {
                readExecutionState(stateByKey, columnCells, seqNr, timestamp);
            } else {
                // TODO unexpected
            }
        }
        
        if (maxSeqNr != null) {
            // TODO
        }
    }
    
    private void readExecutionState(Map<RowLogKey, List<ExecutionData>> stateByKey,
            NavigableMap<Long, byte[]> columnCells, long seqNr, long timestamp) throws IOException {

        for (Map.Entry<Long, byte[]> columnEntry : columnCells.entrySet()) {
            RowLogKey key = new RowLogKey(seqNr, timestamp, columnEntry.getKey());

            List<ExecutionData> states = stateByKey.get(key);
            if (states == null) {
                states = new ArrayList<ExecutionData>();
                stateByKey.put(key, states);
            }

            ExecutionState state = SubscriptionExecutionState.fromBytes(columnEntry.getValue());
            for (CharSequence subscriptionIdCharSeq : state.getSubscriptionIds()) {
                String subscriptionId = subscriptionIdCharSeq.toString();

                ExecutionData data = new ExecutionData();
                data.subscriptionId = subscriptionId;
                data.success = state.getState(subscriptionId);
                states.add(data);
            }

        }
    }

    private void readPayload(Map<RowLogKey, List<String>> payloadByKey, NavigableMap<Long, byte[]> columnCells,
            long seqNr, long timestamp) throws UnsupportedEncodingException {
        for (Map.Entry<Long, byte[]> columnEntry : columnCells.entrySet()) {
            RowLogKey key = new RowLogKey(seqNr, timestamp, columnEntry.getKey());
            List<String> payloads = payloadByKey.get(key);
            if (payloads == null) {
                payloads = new ArrayList<String>();
                payloadByKey.put(key, payloads);
            }
            payloads.add(new String(columnEntry.getValue(), "UTF-8"));
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
        public Long decode(byte[] bytes) {
            return Bytes.toLong(bytes);
        }
    }

    public static class BooleanValueDecoder implements ValueDecoder<Boolean> {
        public Boolean decode(byte[] bytes) {
            return Bytes.toBoolean(bytes);
        }
    }

    public static class StringValueDecoder implements ValueDecoder<String> {
        public String decode(byte[] bytes) {
            return Bytes.toString(bytes);
        }
    }

    public static class Base64ValueDecoder implements ValueDecoder<String> {
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

    private void executeTemplate(String template, Map<String, Object> variables, OutputStream os) throws SAXException {
        DefaultTemplateBuilder builder = new DefaultTemplateBuilder(null, new DefaultTemplateService(), false);
        SourceResolver sourceResolver = new ClasspathSourceResolver();
        Source resource = sourceResolver.resolve(template);
        CompiledTemplate compiledTemplate = builder.buildTemplate(resource);

        TemplateContext context = new DefaultTemplateContext();
        context.putAll(variables);

        ExecutionContext execContext = new ExecutionContext();
        execContext.setTemplateContext(context);
        execContext.setSourceResolver(sourceResolver);
        DefaultTemplateExecutor executor = new DefaultTemplateExecutor();

        TemplateResult result = new TemplateResultImpl(new KauriSaxHandler(os, KauriSaxHandler.OutputFormat.HTML, "UTF-8"));
        executor.execute(compiledTemplate, null, execContext, result);
        result.flush();
    }

}
