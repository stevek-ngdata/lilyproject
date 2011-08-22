package org.lilyproject.tools.recordrowvisualizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.kauriproject.template.*;
import org.kauriproject.template.source.ClasspathSourceResolver;
import org.kauriproject.template.source.Source;
import org.kauriproject.template.source.SourceResolver;
import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.HBaseTypeManager;
import org.lilyproject.repository.impl.IdGeneratorImpl;
import org.lilyproject.repository.impl.SchemaIdImpl;
import org.lilyproject.rowlog.impl.SubscriptionExecutionState;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;
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
    protected Repository repository = null; // TODO

    @Override
    protected String getCmdName() {
        return "lily-record-row";
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        recordIdOption = OptionBuilder
                .withArgName("record-id")
                .isRequired()
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

        IdGenerator idGenerator = new IdGeneratorImpl();
        RecordId recordId = idGenerator.fromString(recordIdString);

        recordRow = new RecordRow();
        recordRow.recordId = recordId;


        // HBase record table
        Configuration conf = HBaseConfiguration.create();
        HTableInterface table = new HTable(conf, Table.RECORD.bytes);

        // Type manager
        // TODO should be able to avoid ZK for this use-case?
        final ZooKeeperItf zk = new StateWatchingZooKeeper(zkConnectionString, zkSessionTimeout);
        typeMgr = new HBaseTypeManager(idGenerator, conf, zk, new HBaseTableFactoryImpl(conf));
        

        Get get = new Get(recordId.toBytes());
        get.setMaxVersions();
        Result row = table.get(get);

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long,byte[]>>> root = row.getMap();

        readSystemFields(root.get(RecordCf.DATA.bytes));

        readFields(root.get(RecordCf.DATA.bytes));

        readRowLog(recordRow.walState, recordRow.walPayload, recordRow.mqState, recordRow.mqPayload, root.get(RecordCf.ROWLOG.bytes));

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

    private boolean isInArray(byte[] key, byte[][] data) {
        for (byte[] item : data) {
            if (Arrays.equals(item, key))
                return true;
        }
        return false;
    }

    private void readSystemFields(NavigableMap<byte[], NavigableMap<Long, byte[]>> cf) throws Exception {
        for (Map.Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : cf.entrySet()) {
            byte[] columnKey = columnEntry.getKey();

            if (Arrays.equals(columnKey, RecordColumn.DELETED.bytes)) {
                setVersionedValue(recordRow.deleted, columnEntry.getValue(), BOOLEAN_DECODER);
            } else if (Arrays.equals(columnKey, RecordColumn.NON_VERSIONED_RT_ID.bytes)) {
                setTypeId(recordRow.nvRecordType, columnEntry.getValue());
            } else if (Arrays.equals(columnKey, RecordColumn.NON_VERSIONED_RT_VERSION.bytes)) {
                setTypeVersion(recordRow.nvRecordType, columnEntry.getValue());
            } else if (Arrays.equals(columnKey, RecordColumn.VERSIONED_RT_ID.bytes)) {
                setTypeId(recordRow.vRecordType, columnEntry.getValue());
            } else if (Arrays.equals(columnKey, RecordColumn.VERSIONED_RT_VERSION.bytes)) {
                    setTypeVersion(recordRow.vRecordType, columnEntry.getValue());
            } else if (Arrays.equals(columnKey, RecordColumn.LOCK.bytes)) {
                setVersionedValue(recordRow.lock, columnEntry.getValue(), BASE64_DECODER);
            } else if (Arrays.equals(columnKey, RecordColumn.VERSION.bytes)) {
                setVersionedValue(recordRow.version, columnEntry.getValue(), LONG_DECODER);
            } else {
                if (columnKey[0] != RecordColumn.DATA_PREFIX) {
                    recordRow.unknownNvColumns.add(Bytes.toString(columnKey));
                }
            }
        }

        for (Type type : recordRow.nvRecordType.getValues().values()) {
            type.object = typeMgr.getRecordTypeById(type.getId(), type.getVersion());
        }
        
        for (Type type : recordRow.vRecordType.getValues().values()) {
            type.object = typeMgr.getRecordTypeById(type.getId(), type.getVersion());
        }
    }

    private void readFields(NavigableMap<byte[], NavigableMap<Long,byte[]>> cf) throws Exception {
        if (cf == null)
            return;

        recordRow.nvFields = new Fields(cf, typeMgr, repository, Scope.NON_VERSIONED);
        recordRow.vFields = new Fields(cf, typeMgr, repository, Scope.VERSIONED);
    }

    private void readRowLog(Map<RowLogKey, List<ExecutionData>> walStateByKey, Map<RowLogKey, List<String>> walPayloadByKey, Map<RowLogKey, List<ExecutionData>> mqStateByKey, Map<RowLogKey, List<String>> mqPayloadByKey, NavigableMap<byte[], NavigableMap<Long, byte[]>> cf) throws IOException {
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
    
    private void readRowLog(Map<RowLogKey, List<ExecutionData>> stateByKey, Map<RowLogKey, List<String>> payloadByKey, NavigableMap<Long, byte[]> columnCells, byte[] key) throws IOException {
        NavigableMap<Long, byte[]> maxSeqNr = null;
        
        if (Arrays.equals(key, SEQ_NR)) { // key could be "SEQNR"
            maxSeqNr = columnCells;
        } else { // or is prefixed by payload or executionState byte
            if (key[0] == PL_BYTE) {
                readPayload(payloadByKey, columnCells, Bytes.toLong(key, 1));
            } else if (key[0] == ES_BYTE) {
                readExecutionState(stateByKey, columnCells, Bytes.toLong(key, 1));
            } else {
                // TODO unexpected
            }
        }
        
        if (maxSeqNr != null) {
            // TODO
        }
    }
    
    private void readExecutionState(Map<RowLogKey, List<ExecutionData>> stateByKey, NavigableMap<Long, byte[]> columnCells, long seqNr) throws IOException {
        for (Map.Entry<Long, byte[]> columnEntry : columnCells.entrySet()) {
            RowLogKey key = new RowLogKey(seqNr, columnEntry.getKey());

            List<ExecutionData> states = stateByKey.get(key);
            if (states == null) {
                states = new ArrayList<ExecutionData>();
                stateByKey.put(key, states);
            }

            SubscriptionExecutionState state = SubscriptionExecutionState.fromBytes(columnEntry.getValue());
            for (CharSequence subscriptionIdCharSeq : state.getSubscriptionIds()) {
                String subscriptionId = subscriptionIdCharSeq.toString();

                ExecutionData data = new ExecutionData();
                data.subscriptionId = subscriptionId;
                data.success = state.getState(subscriptionId);
                states.add(data);
            }

        }
    }

    private void readPayload(Map<RowLogKey, List<String>> payloadByKey, NavigableMap<Long, byte[]> columnCells, long seqNr) throws UnsupportedEncodingException {
        for (Map.Entry<Long, byte[]> columnEntry : columnCells.entrySet()) {
            RowLogKey key = new RowLogKey(seqNr, columnEntry.getKey());
            List<String> payloads = payloadByKey.get(key);
            if (payloads == null) {
                payloads = new ArrayList<String>();
                payloadByKey.put(key, payloads);
            }
            payloads.add(new String(columnEntry.getValue(), "UTF-8"));
        }
    }

    

    
    private void setVersionedValue(VersionedValue value, NavigableMap<Long, byte[]> valuesByVersion, ValueDecoder decoder) {
        for (Map.Entry<Long, byte[]> entry : valuesByVersion.entrySet()) {
            value.put(entry.getKey(), decoder.decode(entry.getValue()));
        }
    }

    private void setTypeId(VersionedValue value, NavigableMap<Long, byte[]> valuesByVersion) {
        for (Map.Entry<Long, byte[]> entry : valuesByVersion.entrySet()) {
            Type type = (Type)value.get(entry.getKey());
            if (type != null) {
                type.id = new SchemaIdImpl(entry.getValue());
            } else {
                type = new Type();
                type.id = new SchemaIdImpl(entry.getValue());
                value.put(entry.getKey(), type);
            }
        }
    }

    private void setTypeVersion(VersionedValue value, NavigableMap<Long, byte[]> valuesByVersion) {
        for (Map.Entry<Long, byte[]> entry : valuesByVersion.entrySet()) {
            Type type = (Type)value.get(entry.getKey());
            if (type != null) {
                type.version = LONG_DECODER.decode(entry.getValue());
            } else {
                type = new Type();
                type.version = LONG_DECODER.decode(entry.getValue());
                value.put(entry.getKey(), type);
            }
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
