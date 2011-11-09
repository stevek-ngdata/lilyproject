package org.lilyproject.tools.linkindex.cli;

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
import org.lilyproject.repository.api.*;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;

import java.util.List;
import java.util.Set;

public class LinkIndexCli extends BaseZkCliTool {
    private Option indexOption;
    private Option recordIdOption;
    private Option vtagOption;
    private Option fieldOption;

    private TypeManager typeManager;

    @Override
    protected String getCmdName() {
        return "lily-linkindex";
    }

    public static void main(String[] args) {
        new LinkIndexCli().start(args);
    }

    @Override
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
                .isRequired()
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

        LilyClient lilyClient = new LilyClient(zkConnectionString, 60000);
        Repository repository = lilyClient.getRepository();
        typeManager = repository.getTypeManager();

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", zkConnectionString);

        IndexManager indexManager = new IndexManager(hbaseConf, new HBaseTableFactoryImpl(hbaseConf));

        LinkIndex linkIndex = new LinkIndex(indexManager, repository);

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
        RecordId recordId = repository.getIdGenerator().fromString(recordIdParam);

        //
        // Determine the vtag
        //
        SchemaId vtagId = null;
        if (cmd.hasOption(vtagOption.getOpt())) {
            String vtagParam = cmd.getOptionValue(vtagOption.getOpt());
            vtagId = repository.getTypeManager().getFieldTypeByName(new QName("org.lilyproject.vtag", vtagParam)).getId();
        }

        //
        // Determine the field
        //
        SchemaId fieldId = null;
        if (cmd.hasOption(fieldOption.getOpt())) {
            if (vtagId == null) {
                System.err.println("A field can also be specified in combination with a vtag.");
                return -1;
            }

            String fieldParam = cmd.getOptionValue(fieldOption.getOpt());
            FieldType field = repository.getTypeManager().getFieldTypeByName(qnameFromString(fieldParam));
            fieldId = field.getId();

            if (!field.getValueType().getPrimitive().getName().equals("LINK")) {
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

        lilyClient.close();

        return 0;
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

    // TODO in Lily 1.1 this is available as QName.fromString
    public static QName qnameFromString(String qname) throws IllegalArgumentException {
        int indexBracket = qname.indexOf('}');
        if (indexBracket < 1 || !qname.startsWith("{"))
            throw new IllegalArgumentException("QName string should be of the format {namespace}name");
        return new QName(qname.substring(1,indexBracket), qname.substring(indexBracket + 1));
    }
}
