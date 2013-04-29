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
package org.lilyproject.tools.import_.cli;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.cli.OptionUtil;
import org.lilyproject.client.LilyClient;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.util.Version;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.lilyproject.util.io.Closer;

public class JsonImportTool extends BaseZkCliTool {
    private Option schemaOnlyOption;
    private Option workersOption;
    private Option quietOption;
    private Option tableOption;
    private Option tenantOption;
    private LilyClient lilyClient;

    @Override
    protected String getCmdName() {
        return "lily-import";
    }

    @Override
    protected String getVersion() {
        return Version.readVersion("org.lilyproject", "lily-import");
    }

    public static void main(String[] args) throws Exception {
        new JsonImportTool().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        workersOption = OptionBuilder
                .withArgName("count")
                .hasArg()
                .withDescription("Number of workers (threads)")
                .withLongOpt("workers")
                .create("w");
        options.add(workersOption);

        schemaOnlyOption = OptionBuilder
                .withDescription("Only import the field types and record types, not the records.")
                .withLongOpt("schema-only")
                .create("s");
        options.add(schemaOnlyOption);

        quietOption = OptionBuilder
                .withDescription("Instead of printing out all record ids, only print a dot every 1000 records")
                .withLongOpt("quiet")
                .create("q");
        options.add(quietOption);

        tableOption = OptionBuilder
                .withArgName("table")
                .hasArg()
                .withDescription("Repository table to import to, defaults to record table")
                .withLongOpt("table")
                .create();
        options.add(tableOption);

        tenantOption = OptionBuilder
                .withArgName("tenant")
                .hasArg()
                .withDescription("Repository tenant, defaults to public tenant")
                .withLongOpt("tenant")
                .create();
        options.add(tenantOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0) {
            return result;
        }

        int workers = OptionUtil.getIntOption(cmd, workersOption, 1);

        String table = OptionUtil.getStringOption(cmd, tableOption, Table.RECORD.name);
        String tenant = OptionUtil.getStringOption(cmd, tenantOption, "public");

        if (cmd.getArgList().size() < 1) {
            System.out.println("No import file specified!");
            return 1;
        }

        boolean schemaOnly = cmd.hasOption(schemaOnlyOption.getOpt());

        lilyClient = new LilyClient(zkConnectionString, zkSessionTimeout);

        for (String arg : (List<String>)cmd.getArgList()) {
            System.out.println("----------------------------------------------------------------------");
            System.out.println("Importing " + arg + " to " + table + " table");
            InputStream is = new FileInputStream(arg);
            try {
                Repository repository = (Repository)lilyClient.getRepository(tenant).getTable(table);
                if (cmd.hasOption(quietOption.getOpt())) {
                    JsonImport.load(repository, new DefaultImportListener(System.out, EntityType.RECORD), is, schemaOnly, workers);
                } else {
                    JsonImport.load(repository, is, schemaOnly, workers);
                }
            } finally {
                Closer.close(is);
            }
        }

        System.out.println("Import done");

        return 0;
    }

    @Override
    protected void cleanup() {
        Closer.close(lilyClient);
        super.cleanup();
    }
}
