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
package org.lilyproject.tools.upgrade;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
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
 * This tool upgrades the Lily schema from Lily 1.0 to 1.1.
 * <p>
 * It performs two upgrades: <br/>
 * - A conversion of a 'null' namespace in record type and field type names.<br/>
 * - A conversion of the value type of the field types to a new encoding
 * including a conversion of multivalue and hierarchical types to LIST and PATH
 * types.
 * <p>
 * Old value type encoding:
 * Bytes.toBytes("primitiveValueTypeName,multivalueBoolean,hierarchyBoolean")
 * <p>
 * New value type encoding: encodingVersionByte( = (byte)1) + utf encoding of
 * value type name (e.g. "LIST<STRING>")
 * <p>
 * This tool should only be run when upgrading from Lily 1.0 to 1.1, and Lily
 * should not be running.
 */
public class UpgradeFrom1_0Tool extends BaseZkCliTool {

    private Option applyOption;
    private Option forceOption;
    private Option upgradeValueTypeOption;
    private Option namespaceOption;
    private Option namespaceValueOption;

    private boolean apply = false;
    private boolean force = false;
    private boolean all = true;
    private boolean namespace = false;
    private boolean upgradeValueType = false;
    private String namespaceValue = "";

    private boolean failure = false;

    @Override
    protected String getCmdName() {
        return "lily-convert-fieldtypes";
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        applyOption = new Option("a", "apply", false, "Applies conversion instead of only reporting it.");
        options.add(applyOption);

        forceOption = new Option("f", "force", false,
                "Force trying each conversion. Other failed conversions are ignored.");
        options.add(forceOption);

        upgradeValueTypeOption = new Option("vt", "valuetype", false, "Enable value type encoding upgrade.");
        options.add(upgradeValueTypeOption);

        namespaceOption = new Option("ns", "namespace", false, "Enable 'null' namespace conversion.");
        options.add(namespaceOption);

        namespaceValueOption = OptionBuilder.withArgName("namespacevalue").hasArg().withDescription(
                "The value to use to replace 'null' namespaces with.")
                .withLongOpt("namespacevalue")
                .create("nsv");
        options.add(namespaceValueOption);

        return options;
    }

    public static void main(String[] args) {
        new UpgradeFrom1_0Tool().start(args);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        inspectOptions(cmd);

        if (apply) {
            System.out.println("Applying schema conversions !!");
            System.out.println();
        } else {
            System.out.println("Executing dry-run of schema conversions!");
            System.out.println("To apply the conversions use the -a option.");
        }

        convertNamespace();
        
        upgradeValueTypes();

        return 0;
    }

    private void inspectOptions(CommandLine cmd) {
        if (cmd.hasOption(applyOption.getOpt())) {
            apply = true;
        }
        if (cmd.hasOption(forceOption.getOpt())) {
            force = true;
        }
        if (cmd.hasOption(upgradeValueTypeOption.getOpt())) {
            all = false;
            upgradeValueType = true;
        }
        if (cmd.hasOption(namespaceOption.getOpt())) {
            all = false;
            namespace = true;
        }
        if (cmd.hasOption(namespaceValueOption.getOpt())) {
            namespaceValue = cmd.getOptionValue(namespaceValueOption.getOpt());
        }
    }

    private void convertNamespace() {
        if (!all && !namespace)
            return;

        System.out.println("\nConvert 'null' namespaces");
        System.out.println("=========================");

        if (failure && !force) {
            System.out.println("A previous conversion failed. Skipping namespace conversion.");
            return;
        }

        try {
            if (!assertLilyNotRunning())
                return;

            HTableInterface typeTable = getTypeTable();

            List<FieldTypeContext> fieldTypesToConvert = collectFieldTypes(typeTable, false);
            List<RecordTypeContext> recordTypesToConvert = collectRecordTypes(typeTable);

            System.out.println("\nConverting namespace of fieldTypes: ");
            for (FieldTypeContext fieldTypeContext : fieldTypesToConvert) {
                fieldTypeContext.convertNamespace(typeTable);
            }

            System.out.println("\nConverting namespace of recordTypes: ");
            for (RecordTypeContext recordTypeContext : recordTypesToConvert) {
                recordTypeContext.convertNamespace(typeTable);
            }

        } catch (InterruptedException e) {
            failure = true;
            System.out.println("Value type upgrade got interrupted. Aborting!");
        } catch (Throwable e) {
            failure = true;
            System.out.println("Value type upgrade encountered an exception. Aborting! " + e.getMessage());
        }
    }

    private void upgradeValueTypes() {
        if (!all && !upgradeValueType)
            return;

        System.out.println("\nUpgrade value type encoding");
        System.out.println("===========================");

        if (failure && !force) {
            System.out.println("A previous conversion failed. Skipping value type upgrade.");
            return;
        }

        try {
            if (!assertLilyNotRunning())
                return;

            HTableInterface typeTable = getTypeTable();

            List<FieldTypeContext> fieldTypesToUpgrade = collectFieldTypes(typeTable, true);

            System.out.println("Fields to convert to value type new encoding: ");
            for (FieldTypeContext fieldTypeContext : fieldTypesToUpgrade) {
                fieldTypeContext.upgradeValueType(apply, typeTable);
            }
        } catch (InterruptedException e) {
            failure = true;
            System.out.println("Value type upgrade got interrupted. Aborting!");
        } catch (Throwable e) {
            failure = true;
            System.out.println("Value type upgrade encountered an exception. Aborting! " + e.getMessage());
        }
    }

    private HTableInterface getTypeTable() throws IOException {
        HTableInterface typeTable;
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkConnectionString);
        HBaseTableFactory tableFactory = new HBaseTableFactoryImpl(conf);
        typeTable = LilyHBaseSchema.getTypeTable(tableFactory);
        return typeTable;
    }
    
    private List<RecordTypeContext> collectRecordTypes(HTableInterface typeTable) throws IOException {
        List<RecordTypeContext> recordTypes = new ArrayList<RecordTypeContext>();

        ResultScanner scanner = typeTable.getScanner(TypeCf.DATA.bytes);
        for (Result scanEntry : scanner) {
            // This covers the case where a given id would match a name that was
            // used for setting the concurrent counters
            if (scanEntry == null || scanEntry.isEmpty()
                    || scanEntry.getValue(TypeCf.DATA.bytes, TypeColumn.RECORDTYPE_NAME.bytes) == null) {
                // Skip
                continue;
            }
            NavigableMap<byte[], byte[]> nonVersionableColumnFamily = scanEntry.getFamilyMap(TypeCf.DATA.bytes);

            // Decode namespace and name
            // Not putting it in a QName since the namespace might still be null
            DataInput dataInput = new DataInputImpl(nonVersionableColumnFamily.get(TypeColumn.RECORDTYPE_NAME.bytes));
            String namespace = dataInput.readUTF();
            String name = dataInput.readUTF();
            recordTypes.add(new RecordTypeContext(scanEntry.getRow(), namespace, name));
        }
        Closer.close(scanner);
        return recordTypes;
    }

    private List<FieldTypeContext> collectFieldTypes(HTableInterface typeTable, boolean valueType) throws IOException {
        List<FieldTypeContext> fieldTypes = new ArrayList<FieldTypeContext>();

        ResultScanner scanner = typeTable.getScanner(TypeCf.DATA.bytes);
        for (Result scanEntry : scanner) {
            // This covers the case where a given id would match a name that was
            // used for setting the concurrent counters
            if (scanEntry == null || scanEntry.isEmpty()
                    || scanEntry.getValue(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_NAME.bytes) == null) {
                // Skip
                continue;
            }
            NavigableMap<byte[], byte[]> nonVersionableColumnFamily = scanEntry.getFamilyMap(TypeCf.DATA.bytes);

            // Decode namespace and name
            // Not putting it in a QName since the namespace might still be null
            DataInput dataInput = new DataInputImpl(nonVersionableColumnFamily.get(TypeColumn.FIELDTYPE_NAME.bytes));
            String namespace = dataInput.readUTF();
            String name = dataInput.readUTF();
            
            // Decode scope
            Scope scope = Scope.valueOf(Bytes
                    .toString(nonVersionableColumnFamily.get(TypeColumn.FIELDTYPE_SCOPE.bytes)));

            String valueTypeName = null;
            boolean multiValue = false;
            boolean hierarchical = false;

            if (valueType) {
                // Decode value type
                byte[] valueTypeBytes = nonVersionableColumnFamily.get(TypeColumn.FIELDTYPE_VALUETYPE.bytes);
                String encodedString = Bytes.toString(valueTypeBytes);
                try {
                    int endOfPrimitiveValueTypeName = encodedString.indexOf(",");
                    valueTypeName = encodedString.substring(0, endOfPrimitiveValueTypeName);
                    int endOfMultiValueBoolean = encodedString.indexOf(",", endOfPrimitiveValueTypeName + 1);
                    multiValue = Boolean.parseBoolean(encodedString.substring(endOfPrimitiveValueTypeName + 1,
                            endOfMultiValueBoolean));
                    hierarchical = Boolean.parseBoolean(encodedString.substring(endOfMultiValueBoolean + 1));
                    fieldTypes.add(new FieldTypeContext(scanEntry.getRow(), namespace, name, scope, valueTypeName,
                            multiValue, hierarchical));
                } catch (Exception e) {
                    System.out.println("Converting value type for field type" + name + " failed with exception: "
                            + e.getMessage());
                    dataInput = new DataInputImpl(valueTypeBytes);
                    if (dataInput.readByte() == HBaseTypeManager.valueTypeEncodingVersion) {
                        System.out.println("It seems Lily 1.1 encoding is already used.");
                        System.out.println("According to this encoding the value type is: " + dataInput.readUTF());
                    }
                }
            } else {
                fieldTypes.add(new FieldTypeContext(scanEntry.getRow(), namespace, name, scope, valueTypeName,
                        multiValue, hierarchical));
            }
        }
        Closer.close(scanner);
        return fieldTypes;
    }

    private boolean assertLilyNotRunning() throws Exception {
        ZooKeeperItf zk = ZkUtil.connect(zkConnectionString, zkSessionTimeout);
        List<String> lilyServers = zk.getChildren("/lily/repositoryNodes", true);
        if (!lilyServers.isEmpty()) {
            System.out.println("WARNING! Lily should not be running when performing this conversion.");
            System.out.println("         Only HBase, Hadoop and zookeeper should be running.");
            failure = true;
            return false;
        }
        return true;
    }

    private class FieldTypeContext {
        byte[] id;
        String namespace;
        String name;
        String valueTypeName;
        boolean multiValue;
        boolean hierarchical;
        private final Scope scope;

        FieldTypeContext(byte[] id, String namespace, String name, Scope scope, String valueTypeName, boolean multiValue,
                boolean hierarchical) {
            this.id = id;
            this.namespace = namespace;
            this.name = name;
            this.scope = scope;
            this.valueTypeName = valueTypeName;
            this.multiValue = multiValue;
            this.hierarchical = hierarchical;
        }

        public void upgradeValueType(boolean apply, HTableInterface typeTable) throws IOException {
            String newValueTypeName = valueTypeName;
            if (hierarchical) {
                newValueTypeName = "PATH<" + newValueTypeName + ">";
            }
            if (multiValue) {
                newValueTypeName = "LIST<" + newValueTypeName + ">";
            }
            // Show what will be changed
            StringBuilder builder = new StringBuilder();
            builder.append("{").append(namespace).append("}").append(name).append(", ").append(scope).append(" : ");
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

        public void convertNamespace(HTableInterface typeTable) throws Exception {
            StringBuilder builder = new StringBuilder();
            builder.append("{").append(namespace).append("}").append(name);
            if (namespace == null) {
                builder.append(" -> {").append(namespaceValue).append("}").append(name);
            } else {
                builder.append(" -> namespace not 'null': keeping namespace");
            }
            System.out.println(builder.toString());
            if (apply) {
                Put put = new Put(id);
                byte[] encodedName = HBaseTypeManager.encodeName(new QName(namespaceValue, name));
                put.add(TypeCf.DATA.bytes, TypeColumn.FIELDTYPE_NAME.bytes, encodedName);
                typeTable.put(put);
            }
        }


    }
    
    private class RecordTypeContext {
        private byte[] id;
        private String namespace;
        private String name;
        
        RecordTypeContext(byte[] id, String namespace, String name) {
            this.id = id;
            this.namespace = namespace;
            this.name = name;
        }

        public void convertNamespace(HTableInterface typeTable) throws Exception {
            StringBuilder builder = new StringBuilder();
            builder.append("{").append(namespace).append("}").append(name);
            if (namespace == null) {
                builder.append(" -> {").append(namespaceValue).append("}").append(name);
            } else {
                builder.append(" -> namespace not 'null': keeping namespace");
            }
            System.out.println(builder.toString());
            if (apply) {
                Put put = new Put(id);
                byte[] encodedName = HBaseTypeManager.encodeName(new QName(namespaceValue, name));
                put.add(TypeCf.DATA.bytes, TypeColumn.RECORDTYPE_NAME.bytes, encodedName);
                typeTable.put(put);
            }
        }
    }
}
