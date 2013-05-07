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
package org.lilyproject.tools.linkindex.cli;

import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.client.LilyClient;
import org.lilyproject.hbaseindex.IndexManager;
import org.lilyproject.linkindex.FieldedLink;
import org.lilyproject.linkindex.LinkIndex;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.util.Version;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;
import org.lilyproject.util.io.Closer;

public class LinkIndexCli extends BaseZkCliTool {
    private Option indexOption;
    private Option recordIdOption;
    private Option vtagOption;
    private Option fieldOption;

    private LilyClient lilyClient;
    private TypeManager typeManager;

    @Override
    protected String getCmdName() {
        return "lily-query-linkindex";
    }

    public static void main(String[] args) {
        new LinkIndexCli().start(args);
    }

    @Override
    protected String getVersion() {
        return Version.readVersion("org.lilyproject", "lily-linkindex-cli");
    }

    @Override
    @SuppressWarnings("static-access")
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        indexOption = OptionBuilder
                .withArgName("name")
                .hasArg()
                .withDescription("Index name: either 'incoming' (default, synonym 'backward') or 'outgoing' (synonym: 'forward')")
                .withLongOpt("index-name")
                .create("in");
        options.add(indexOption);

        recordIdOption = OptionBuilder
                .withArgName("record_id")
                .hasArg()
                .withDescription("Record ID: UUID.something or USER.something")
                .withLongOpt("record")
                .create("r");
        options.add(recordIdOption);

        vtagOption = OptionBuilder
                .withArgName("vtag")
                .hasArg()
                .withDescription("vtag name, for example: last")
                .withLongOpt("vtag")
                .create("vt");
        options.add(vtagOption);

        fieldOption = OptionBuilder
                .withArgName("field")
                .hasArg()
                .withDescription("field name, format: {namespace}name")
                .withLongOpt("field")
                .create("f");
        options.add(fieldOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0) {
            return result;
        }

        lilyClient = new LilyClient(zkConnectionString, 60000);
        LRepository repository = lilyClient.getDefaultRepository();
        typeManager = repository.getTypeManager();

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", zkConnectionString);

        IndexManager indexManager = new IndexManager(hbaseConf, new HBaseTableFactoryImpl(hbaseConf));

        LinkIndex linkIndex = new LinkIndex(indexManager, lilyClient);

        //
        // Determine the index to query
        //
        boolean incoming = true; // false = outgoing
        if (cmd.hasOption(indexOption.getOpt())) {
            String index = cmd.getOptionValue(indexOption.getOpt());
            if (index.equals("incoming") || index.equals("backward")) {
                incoming = true;
            } else if (index.equals("outgoing") || index.equals("forward")) {
                incoming = false;
            } else {
                System.err.println("Invalid index name: " + index);
                return -1;
            }
        }

        //
        // Determine the record id
        //
        String recordIdParam = cmd.getOptionValue(recordIdOption.getOpt());
        if (recordIdParam == null) {
            System.out.println("Specify record id with -" + recordIdOption.getOpt());
            return 1;
        }
        RecordId recordId = repository.getIdGenerator().fromString(recordIdParam);

        //
        // Determine the vtag
        //
        SchemaId vtagId = null;
        if (cmd.hasOption(vtagOption.getOpt())) {
            String vtagParam = cmd.getOptionValue(vtagOption.getOpt());
            vtagId = typeManager.getFieldTypeByName(new QName("org.lilyproject.vtag", vtagParam)).getId();
        }

        //
        // Determine the field
        //
        SchemaId fieldId = null;
        if (cmd.hasOption(fieldOption.getOpt())) {
            if (vtagId == null) {
                System.err.println("A field can only be specified in combination with a vtag.");
                return -1;
            }

            String fieldParam = cmd.getOptionValue(fieldOption.getOpt());
            FieldType field = typeManager.getFieldTypeByName(QName.fromString(fieldParam));
            fieldId = field.getId();

            if (!field.getValueType().getDeepestValueType().getBaseName().equals("LINK")) {
                System.err.println("The field '" + field.getName() + "' is not a link field.");
                return -1;
            }
        }

        //
        // Perform the query
        //
        if (incoming) {
            System.out.println("Querying the incoming links (backward index)");
            System.out.println("Record id: " + recordId);
            System.out.println("vtag id: " + vtagId + getFieldTypeNameSuffix(vtagId));
            System.out.println("field id: " + fieldId + getFieldTypeNameSuffix(fieldId));
            System.out.println();
            if (fieldId != null) {
                Set<RecordId> records = linkIndex.getReferrers(recordId, vtagId, fieldId);
                printRecords(records);
            } else {
                Set<FieldedLink> fieldedLinks = linkIndex.getFieldedReferrers(recordId, vtagId);
                printFieldedLinks(fieldedLinks);
            }
        } else {
            System.out.println("Querying the outgoing links (forward index)");
            System.out.println("Record id: " + recordId);
            System.out.println("vtag id: " + vtagId + getFieldTypeNameSuffix(vtagId));
            System.out.println("field id: " + fieldId + getFieldTypeNameSuffix(fieldId));
            System.out.println();
            if (fieldId != null) {
                Set<RecordId> records = linkIndex.getForwardLinks(recordId, vtagId, fieldId);
                printRecords(records);
            } else {
                Set<FieldedLink> fieldedLinks = linkIndex.getFieldedForwardLinks(recordId, vtagId);
                printFieldedLinks(fieldedLinks);
            }
        }

        return 0;
    }

    @Override
    protected void cleanup() {
        Closer.close(lilyClient);
        super.cleanup();
    }

    private String getFieldTypeNameSuffix(SchemaId id) throws RepositoryException, InterruptedException {
        if (id == null) {
            return "";
        } else {
            return " -- " + typeManager.getFieldTypeById(id).getName();
        }
    }

    private void printRecords(Set<RecordId> records) {
        System.out.println("Result count: " + records.size());
        System.out.println();

        for (RecordId record : records) {
            System.out.println(record);
        }
    }

    private void printFieldedLinks(Set<FieldedLink> fieldedLinks) throws RepositoryException, InterruptedException {
        System.out.println("Result count: " + fieldedLinks.size());
        System.out.println();

        for (FieldedLink link : fieldedLinks) {
            QName fieldName = typeManager.getFieldTypeById(link.getFieldTypeId()).getName();
            System.out.println(link.getRecordId() + " -- " + fieldName);
        }
    }
}
