/*
 * Copyright 2011 Outerthought bvba
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
package org.lilyproject.tools.convertfieldtypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.impl.HBaseTypeManager;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.LilyHBaseSchema.TypeCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.TypeColumn;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

/**
 * This tool converts the encoding of the FieldTypes from Lily 1.0 to 1.1
 * encoding.
 * <p>
 * This tool should only be run when upgrading from Lily 1.0 to 1.1, and Lily
 * should not be running.
 * <p>
 * More specifically it converts the value type column to a new encoding and
 * changes multivalue and hierarchical types to LIST and PATH types
 * respectively.
 * <p>
 * Old value type encoding:
 * Bytes.toBytes("primitiveValueTypeName,multivalueBoolean,hierarchyBoolean")
 * <p>
 * New value type encoding: encodingVersionByte( = (byte)1) + utf encoding of
 * value type name (e.g. "LIST<STRING>")
 */
public class ConvertFieldTypesTool extends BaseZkCliTool {

    private HTableInterface typeTable;
    private List<FieldTypeContext> fieldTypesToConvert = new ArrayList<FieldTypeContext>();
    private ZooKeeperItf zk;
    private Option applyOption;
    private boolean apply;

    @Override
    protected String getCmdName() {
        return "lily-convert-fieldtypes";
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();
        applyOption = new Option("a", "apply", false, "Applies conversion instead of only reporting it.");
        options.add(applyOption);

        return options;
    }

    public static void main(String[] args) {
        new ConvertFieldTypesTool().start(args);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        if (cmd.hasOption(applyOption.getOpt())) {
            apply = true;
            System.out.println("Applying field type conversion !!");
            System.out.println();
        } else {
            System.out.println("Field type conversion: dry-run!");
            System.out.println("To apply the conversion use the -a option.");
        }

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkConnectionString);

        HBaseTableFactory tableFactory = new HBaseTableFactoryImpl(conf);
        typeTable = LilyHBaseSchema.getTypeTable(tableFactory);

        zk = ZkUtil.connect(zkConnectionString, zkSessionTimeout);
        List<String> lilyServers = zk.getChildren("/lily/repositoryNodes", true);
        if (!lilyServers.isEmpty()) {
            System.out.println("WARNING! Lily should not be running when performing this conversion.");
            System.out.println("         Only HBase, Hadoop and zookeeper should be running.");
            return 0;
        }

        ResultScanner scanner;
        try {
            scanner = typeTable.getScanner(TypeCf.DATA.bytes);
            for (Result scanEntry : scanner) {
                collectFieldType(scanEntry);
            }
        } catch (IOException e) {
            throw new Exception("Exception occurred while collecting FieldTypes to convert", e);
        }
        Closer.close(scanner);

        System.out.println("Fields to convert to new encoding: ");
        for (FieldTypeContext fieldTypeContext : fieldTypesToConvert) {
            fieldTypeContext.convert(apply);
        }

        return 0;
    }
    
    private void collectFieldType(Result result) {
        // This covers the case where a given id would match a name that was
        // used for setting the concurrent counters
        if (result == null || result.isEmpty()
                || result.getValue(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_NAME.bytes) == null) {
            // Skip
            return;
        }
        NavigableMap<byte[], byte[]> nonVersionableColumnFamily = result.getFamilyMap(TypeCf.DATA.bytes);
        QName name = HBaseTypeManager.decodeName(nonVersionableColumnFamily.get(TypeColumn.FIELDTYPE_NAME.bytes));
        Scope scope = Scope.valueOf(Bytes.toString(nonVersionableColumnFamily.get(TypeColumn.FIELDTYPE_SCOPE.bytes)));

        byte[] valueTypeBytes = nonVersionableColumnFamily.get(TypeColumn.FIELDTYPE_VALUETYPE.bytes);
        String encodedString = Bytes.toString(valueTypeBytes);
        try {
            int endOfPrimitiveValueTypeName = encodedString.indexOf(",");
            String valueTypeName = encodedString.substring(0, endOfPrimitiveValueTypeName);
            int endOfMultiValueBoolean = encodedString.indexOf(",", endOfPrimitiveValueTypeName + 1);
            boolean multiValue = Boolean.parseBoolean(encodedString.substring(endOfPrimitiveValueTypeName + 1,
                    endOfMultiValueBoolean));
            boolean hierarchical = Boolean.parseBoolean(encodedString.substring(endOfMultiValueBoolean + 1));
            fieldTypesToConvert.add(new FieldTypeContext(result.getRow(), name, scope, valueTypeName, multiValue,
                    hierarchical));
        } catch (Exception e) {
            System.out.println("Converting value type for field type" + name + " failed with exception: "
                    + e.getMessage());
            DataInput dataInput = new DataInputImpl(valueTypeBytes);
            if (dataInput.readByte() == HBaseTypeManager.valueTypeEncodingVersion) {
                System.out.println("It seems Lily 1.1 encoding is already used.");
                System.out.println("According to this encoding the value type is: " + dataInput.readUTF());
            }
        }
    }

    private class FieldTypeContext {
        byte[] id;
        QName name;
        String valueTypeName;
        boolean multiValue;
        boolean hierarchical;
        private final Scope scope;

        FieldTypeContext(byte[] id, QName name, Scope scope, String valueTypeName, boolean multiValue,
                boolean hierarchical) {
            this.id = id;
            this.name = name;
            this.scope = scope;
            this.valueTypeName = valueTypeName;
            this.multiValue = multiValue;
            this.hierarchical = hierarchical;
        }

        public void convert(boolean apply) throws IOException {
            String newValueTypeName = valueTypeName;
            if (hierarchical) {
                newValueTypeName = "PATH<" + newValueTypeName + ">";
            }
            if (multiValue) {
                newValueTypeName = "LIST<" + newValueTypeName + ">";
            }
            // Show what will be changed
            StringBuilder builder = new StringBuilder();
            builder.append(name).append(", ").append(scope).append(" : ");
            builder.append(valueTypeName);
            if (multiValue)
                builder.append(", multivalue");
            if (hierarchical)
                builder.append(", hierarchical");
            builder.append(" -> ").append(newValueTypeName);
            System.out.println(builder.toString());

            // Apply the change
            if (apply) {
                DataOutput dataOutput = new DataOutputImpl();
                dataOutput.writeByte(HBaseTypeManager.valueTypeEncodingVersion);
                dataOutput.writeUTF(newValueTypeName);
                Put put = new Put(id);
                put.add(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_VALUETYPE.bytes, HBaseTypeManager.encodeValueType(newValueTypeName));
                typeTable.put(put);
            }
        }
    }
}
